import getLogger from '../utils/logger.js';
import { v4 as uuidv4 } from 'uuid';

const logger = getLogger('core/Coordinator');

/**
 * Coordinator class responsible for managing consumer distribution and queue assignments
 * based on priority weights. Uses etcd for distributed coordination.
 */
export default class Coordinator {
  /**
   * Create a new Coordinator instance
   * @param {EtcdClient} etcdClient - etcd client instance
   * @param {AppConfig} config - System configuration
   */
  constructor(etcdClient, config) {
    this._etcdClient = etcdClient;
    this._config = config;
    this._options = config.coordinator;

    // State tracking
    this._consumerId = null;
    this._lease = null;
    this._heartbeatInterval = null;
    this._watchers = new Map();
    this._assignedQueues = new Set();
    this._queuePriorities = new Map();

    logger.info('Coordinator initialized');
  }

  /**
   * Register this consumer with etcd
   * @param {string} [consumerId] - Optional consumer ID (generated if not provided)
   * @returns {Promise<string>} - Assigned consumer ID
   */
  async register(consumerId = null) {
    try {
      // Use provided ID or generate one
      this._consumerId = consumerId || this._generateConsumerId();

      // Create a lease for this consumer
      this._lease = await this._etcdClient.createLease(this._options.leaseTTLInSeconds);

      // Register consumer with lease
      const consumerKey = `${this._options.consumerPrefix}${this._consumerId}`;
      await this._etcdClient.set(
        consumerKey,
        JSON.stringify({
          registered: Date.now(),
          lastHeartbeat: Date.now(),
        }),
        this._lease
      );

      // Start heartbeat to keep lease alive
      this._startHeartbeat();

      // Watch for queue assignments
      await this._watchQueueAssignments();

      logger.info(`Registered consumer with ID: ${this._consumerId}`);
      return this._consumerId;
    } catch (error) {
      logger.error('Error registering consumer:', error);
      throw error;
    }
  }

  /**
   * Deregister this consumer from etcd
   * @returns {Promise<boolean>} - Success indicator
   */
  async deregister() {
    if (!this._consumerId) {
      logger.warn('Cannot deregister: Consumer not registered');
      return false;
    }

    try {
      // Stop heartbeat
      this._stopHeartbeat();

      // Remove consumer registration
      const consumerKey = `${this._options.consumerPrefix}${this._consumerId}`;
      await this._etcdClient.delete(consumerKey);

      // Clean up watchers
      for (const watcher of this._watchers.values()) {
        watcher.cancel();
      }
      this._watchers.clear();

      // Clear assignments
      this._assignedQueues.clear();

      logger.info(`Deregistered consumer ${this._consumerId}`);
      return true;
    } catch (error) {
      logger.error(`Error deregistering consumer ${this._consumerId}:`, error);
      return false;
    }
  }

  /**
   * Set the priority for a queue
   * @param {string} queueName - Queue name
   * @param {number} priority - Priority value (higher = more important)
   * @returns {Promise<boolean>} - Success indicator
   */
  async setQueuePriority(queueName, priority) {
    try {
      const queueConfigKey = `${this._options.queueConfigPrefix}${queueName}`;
      const config = {
        priority,
        updatedAt: Date.now(),
      };

      await this._etcdClient.set(queueConfigKey, JSON.stringify(config));
      this._queuePriorities.set(queueName, priority);

      logger.info(`Set queue ${queueName} priority to ${priority}`);
      return true;
    } catch (error) {
      logger.error(`Error setting priority for queue ${queueName}:`, error);
      return false;
    }
  }

  /**
   * Get the priority for a queue
   * @param {string} queueName - Queue name
   * @returns {Promise<number>} - Priority value (default: 1)
   */
  async getQueuePriority(queueName) {
    // Check local cache first
    if (this._queuePriorities.has(queueName)) {
      return this._queuePriorities.get(queueName);
    }

    try {
      const queueConfigKey = `${this._options.queueConfigPrefix}${queueName}`;
      const configJson = await this._etcdClient.get(queueConfigKey);

      if (configJson) {
        const config = JSON.parse(configJson);
        this._queuePriorities.set(queueName, config.priority);
        return config.priority;
      }

      // If priority not found in etcd, use config-based calculation
      const calculatedPriority = this._config.calculateQueuePriority(queueName);
      this._queuePriorities.set(queueName, calculatedPriority);

      // Store the calculated priority in etcd for future use
      await this.setQueuePriority(queueName, calculatedPriority);

      return calculatedPriority;
    } catch (error) {
      logger.error(`Error getting priority for queue ${queueName}:`, error);
      return 1; // Default priority on error
    }
  }

  /**
   * Get all registered consumers
   * @returns {Promise<Array<string>>} - Array of consumer IDs
   */
  async getConsumers() {
    try {
      const consumers = await this._etcdClient.getWithPrefix(this._options.consumerPrefix);
      return Object.keys(consumers).map(key => key.replace(this._options.consumerPrefix, ''));
    } catch (error) {
      logger.error('Error getting consumers:', error);
      return [];
    }
  }

  /**
   * Get all registered queues with priorities
   * @returns {Promise<Object>} - Map of queue names to priorities
   */
  async getQueuesWithPriorities() {
    try {
      const queueConfigs = await this._etcdClient.getWithPrefix(this._options.queueConfigPrefix);
      const queuesWithPriorities = {};

      for (const [key, valueJson] of Object.entries(queueConfigs)) {
        const queueName = key.replace(this._options.queueConfigPrefix, '');
        const config = JSON.parse(valueJson);
        queuesWithPriorities[queueName] = config.priority;

        // Update local cache
        this._queuePriorities.set(queueName, config.priority);
      }

      return queuesWithPriorities;
    } catch (error) {
      logger.error('Error getting queues with priorities:', error);
      return {};
    }
  }

  /**
   * Calculate the optimal distribution of consumers to queues based on priorities
   * @param {Array<string>} queues - Available queue names
   * @param {Array<string>} consumers - Available consumer IDs
   * @returns {Promise<Object>} - Mapping of consumers to queues
   */
  async calculateConsumerDistribution(queues, consumers) {
    try {
      // Simplified distribution algorithm
      const consumerAssignments = {};
      const queuePriorities = {};

      // Initialize consumer assignments
      for (const consumer of consumers) {
        consumerAssignments[consumer] = [];
      }

      // If no queues or consumers, return empty assignments
      if (queues.length === 0 || consumers.length === 0) {
        return consumerAssignments;
      }

      // Get queue priorities
      for (const queue of queues) {
        queuePriorities[queue] = await this.getQueuePriority(queue);
      }

      // Sort queues by priority (highest first)
      const sortedQueues = [...queues].sort((a, b) => queuePriorities[b] - queuePriorities[a]);

      // Case 1: More consumers than queues - assign each queue to at least one consumer
      if (consumers.length >= queues.length) {
        return this._distributeConsumersToQueues(
          sortedQueues,
          consumers,
          queuePriorities,
          consumerAssignments
        );
      }
      // Case 2: More queues than consumers - distribute queues evenly
      else {
        return this._distributeQueuesToConsumers(sortedQueues, consumers, consumerAssignments);
      }
    } catch (error) {
      logger.error('Error calculating consumer distribution:', error);
      throw error;
    }
  }

  /**
   * Apply consumer distribution by updating assignments in etcd
   * @param {Object} distribution - Mapping of consumers to queues
   * @returns {Promise<boolean>} - Success indicator
   */
  async applyConsumerDistribution(distribution) {
    try {
      // Process each consumer's assignment
      for (const [consumerId, queues] of Object.entries(distribution)) {
        const assignmentKey = `${this._options.queueAssignmentPrefix}${consumerId}`;
        await this._etcdClient.set(assignmentKey, JSON.stringify(queues));
      }

      logger.info('Applied consumer distribution');
      return true;
    } catch (error) {
      logger.error('Error applying consumer distribution:', error);
      return false;
    }
  }

  /**
   * Rebalance consumers across queues based on priorities
   * @param {Array<string>} queues - Available queue names
   * @returns {Promise<boolean>} - Success indicator
   */
  async rebalanceConsumers(queues) {
    try {
      // Get all active consumers
      const consumers = await this.getConsumers();
      if (consumers.length === 0) {
        logger.warn('No consumers available for rebalancing');
        return false;
      }

      // Calculate optimal distribution
      const distribution = await this.calculateConsumerDistribution(queues, consumers);

      // Apply the distribution
      await this.applyConsumerDistribution(distribution);

      logger.info(`Rebalanced ${consumers.length} consumers across ${queues.length} queues`);
      return true;
    } catch (error) {
      logger.error('Error rebalancing consumers:', error);
      return false;
    }
  }

  /**
   * Get the queues assigned to this consumer
   * @returns {Promise<Array<string>>} - Array of assigned queue names
   */
  async getAssignedQueues() {
    if (!this._consumerId) {
      logger.warn('Cannot get assigned queues: Consumer not registered');
      return [];
    }

    try {
      const assignmentKey = `${this._options.queueAssignmentPrefix}${this._consumerId}`;
      const assignmentJson = await this._etcdClient.get(assignmentKey);

      if (assignmentJson) {
        const queues = JSON.parse(assignmentJson);
        this._assignedQueues = new Set(queues);
        return queues;
      }

      return [];
    } catch (error) {
      logger.error(`Error getting assigned queues for consumer ${this._consumerId}:`, error);
      return [];
    }
  }

  /**
   * Check if a queue is assigned to this consumer
   * @param {string} queueName - Queue name to check
   * @returns {Promise<boolean>} - True if queue is assigned
   */
  async isQueueAssigned(queueName) {
    if (this._assignedQueues.has(queueName)) {
      return true;
    }

    const assignedQueues = await this.getAssignedQueues();
    return assignedQueues.includes(queueName);
  }

  /**
   * Explicitly assign queues to this consumer (for testing purposes)
   * @param {Array<string>} queues - Queue names to assign
   * @returns {Promise<boolean>} - Success indicator
   */
  async assignQueues(queues) {
    if (!this._consumerId) {
      logger.warn('Cannot assign queues: Consumer not registered');
      return false;
    }

    try {
      const assignmentKey = `${this._options.queueAssignmentPrefix}${this._consumerId}`;
      await this._etcdClient.set(assignmentKey, JSON.stringify(queues));

      // Update local cache
      this._assignedQueues = new Set(queues);

      // Notify listeners
      this.onAssignmentChanged(queues);

      logger.info(
        `Explicitly assigned ${queues.length} queues to consumer ${this._consumerId}: ${queues.join(', ')}`
      );
      return true;
    } catch (error) {
      logger.error(`Error assigning queues to consumer ${this._consumerId}:`, error);
      return false;
    }
  }

  /**
   * Distribute consumers to queues when there are more consumers than queues
   * @param {Array<string>} sortedQueues - Queues sorted by priority
   * @param {Array<string>} consumers - Available consumer IDs
   * @param {Object} queuePriorities - Map of queue names to priority values
   * @param {Object} consumerAssignments - Initial assignments object
   * @returns {Object} - Updated consumer assignments
   * @private
   */
  _distributeConsumersToQueues(sortedQueues, consumers, queuePriorities, consumerAssignments) {
    // First, make sure each queue has at least one consumer
    for (let i = 0; i < sortedQueues.length; i++) {
      const consumerIndex = i % consumers.length;
      consumerAssignments[consumers[consumerIndex]].push(sortedQueues[i]);
    }

    // Then distribute remaining consumers based on queue priority
    let consumerIndex = sortedQueues.length % consumers.length;
    const remainingConsumers = consumers.length - consumerIndex;

    if (remainingConsumers > 0) {
      // Create a distribution plan based on priority
      const queueRatios = {};
      let totalPriority = 0;

      for (const queue of sortedQueues) {
        totalPriority += queuePriorities[queue];
      }

      for (const queue of sortedQueues) {
        // Calculate target consumer count based on priority
        const ratio = queuePriorities[queue] / totalPriority;
        const targetCount = Math.max(1, Math.round(ratio * consumers.length));
        queueRatios[queue] = {
          targetCount,
          currentCount: 1, // Each queue already has one consumer
        };
      }

      // Assign remaining consumers to high priority queues first
      for (let i = 0; i < remainingConsumers; i++) {
        // Find the queue with the highest priority that needs more consumers
        let bestQueue = null;
        let bestDifference = -1;

        for (const queue of sortedQueues) {
          const ratio = queueRatios[queue];
          const difference = ratio.targetCount - ratio.currentCount;

          if (difference > 0 && (bestQueue === null || difference > bestDifference)) {
            bestQueue = queue;
            bestDifference = difference;
          }
        }

        // If no queue needs more consumers, assign to highest priority queue
        if (bestQueue === null) {
          bestQueue = sortedQueues[0];
        }

        // Assign to the consumer
        consumerAssignments[consumers[consumerIndex]].push(bestQueue);
        queueRatios[bestQueue].currentCount++;

        consumerIndex = (consumerIndex + 1) % consumers.length;
      }
    }

    return consumerAssignments;
  }

  /**
   * Distribute queues to consumers when there are more queues than consumers
   * @param {Array<string>} sortedQueues - Queues sorted by priority
   * @param {Array<string>} consumers - Available consumer IDs
   * @param {Object} consumerAssignments - Initial assignments object
   * @returns {Object} - Updated consumer assignments
   * @private
   */
  _distributeQueuesToConsumers(sortedQueues, consumers, consumerAssignments) {
    const queuesPerConsumer = Math.ceil(sortedQueues.length / consumers.length);

    // Distribute queues to consumers
    for (let i = 0; i < sortedQueues.length; i++) {
      const consumerIndex = Math.floor(i / queuesPerConsumer);
      if (consumerIndex < consumers.length) {
        consumerAssignments[consumers[consumerIndex]].push(sortedQueues[i]);
      } else {
        // If we have more queues than can be evenly distributed,
        // assign overflow to the first consumer (which should have highest capacity)
        consumerAssignments[consumers[0]].push(sortedQueues[i]);
      }
    }

    return consumerAssignments;
  }

  /**
   * Group queues by priority when there are more queues than consumers
   * @param {Array<string>} queues - Queue names
   * @param {Object} queuePriorities - Queue priorities
   * @param {number} consumerCount - Available consumer count
   * @returns {Object} - Grouped queues
   * @private
   */
  _groupQueuesByPriority(queues, queuePriorities, consumerCount) {
    const consumerAssignments = {};

    // Initialize consumer assignments
    for (let i = 0; i < consumerCount; i++) {
      consumerAssignments[`consumer-${i}`] = [];
    }

    // If enough consumers for all queues, assign one-to-one
    if (queues.length <= consumerCount) {
      queues.forEach((queue, index) => {
        consumerAssignments[`consumer-${index}`].push(queue);
      });
      return consumerAssignments;
    }

    // Sort queues by priority (highest first)
    const sortedQueues = [...queues].sort((a, b) => queuePriorities[b] - queuePriorities[a]);

    // Split into high priority and low priority
    const highPriorityCount = Math.min(consumerCount, sortedQueues.length);
    const highPriorityQueues = sortedQueues.slice(0, highPriorityCount);
    const lowPriorityQueues = sortedQueues.slice(highPriorityCount);

    // Assign high-priority queues first (one per consumer)
    highPriorityQueues.forEach((queue, index) => {
      consumerAssignments[`consumer-${index}`].push(queue);
    });

    // Distribute low-priority queues among available consumers using round-robin
    lowPriorityQueues.forEach((queue, index) => {
      const consumerIndex = index % consumerCount;
      consumerAssignments[`consumer-${consumerIndex}`].push(queue);
    });

    logger.debug(
      `Grouped ${lowPriorityQueues.length} low-priority queues among ${consumerCount} consumers`
    );

    return consumerAssignments;
  }

  /**
   * Start heartbeat to keep consumer lease alive
   * @private
   */
  _startHeartbeat() {
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval);
    }

    this._heartbeatInterval = setInterval(async () => {
      try {
        // Keep lease alive
        await this._lease.keepalive();

        // Update heartbeat timestamp
        const consumerKey = `${this._options.consumerPrefix}${this._consumerId}`;
        await this._etcdClient.set(
          consumerKey,
          JSON.stringify({
            registered: Date.now(),
            lastHeartbeat: Date.now(),
          }),
          this._lease
        );

        logger.debug(`Sent heartbeat for consumer ${this._consumerId}`);
      } catch (error) {
        logger.error(`Error sending heartbeat for consumer ${this._consumerId}:`, error);
      }
    }, this._options.heartbeatIntervalInMs);
  }

  /**
   * Stop heartbeat for consumer lease
   * @private
   */
  _stopHeartbeat() {
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval);
      this._heartbeatInterval = null;
    }
  }

  /**
   * Watch for queue assignment changes
   * @private
   */
  async _watchQueueAssignments() {
    if (!this._consumerId) {
      logger.warn('Cannot watch assignments: Consumer not registered');
      return;
    }

    try {
      const assignmentKey = `${this._options.queueAssignmentPrefix}${this._consumerId}`;

      // Set up watcher
      const watcher = await this._etcdClient.watchPrefix(
        assignmentKey,
        async (event, key, value) => {
          if (event === 'put') {
            const queues = JSON.parse(value);
            this._assignedQueues = new Set(queues);
            logger.info(`Queue assignments updated for consumer ${this._consumerId}:`, queues);

            // Emit event so QueueManager can react
            this.onAssignmentChanged(queues);
          } else if (event === 'delete') {
            this._assignedQueues.clear();
            logger.info(`Queue assignments removed for consumer ${this._consumerId}`);

            // Emit event so QueueManager can react
            this.onAssignmentChanged([]);
          }
        }
      );

      this._watchers.set('assignments', watcher);
      logger.debug(`Started watching queue assignments for consumer ${this._consumerId}`);
    } catch (error) {
      logger.error(`Error watching queue assignments for consumer ${this._consumerId}:`, error);
    }
  }

  /**
   * Generate a unique consumer ID
   * @returns {string} - Generated consumer ID
   * @private
   */
  _generateConsumerId() {
    const hostname = process.env.HOSTNAME || 'unknown';
    const timestamp = Date.now();
    return `CONSUMER-${hostname}-${timestamp}-${uuidv4()}`;
  }

  /**
   * Event handler for queue assignment changes
   * Override this method to handle assignment changes
   * @param {Array<string>} queues - New queue assignments
   */
  onAssignmentChanged(queues) {
    logger.debug(`Assignment changed event: ${queues.join(', ')}`);
  }
}
