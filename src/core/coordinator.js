import getLogger from '../utils/logger.js';
import etcdClient from '../client/etcdClient.js';
import { v4 as uuidv4 } from 'uuid';

const logger = getLogger('core/Coordinator');

/**
 * Coordinator class responsible for managing consumer distribution and queue assignments
 * based on priority weights. Uses etcd for distributed coordination.
 */
class Coordinator {
  /**
   * Create a new Coordinator instance
   * @param {Object} options - Configuration options
   */
  constructor(options = {}) {
    if (Coordinator.instance) {
      return Coordinator.instance;
    }
    this._etcdClient = etcdClient;
    this._options = {
      consumerPrefix: '/rabbitmq-consumers/',
      queueAssignmentPrefix: '/queue-assignments/',
      queueConfigPrefix: '/queue-configs/',
      leaseTTLInSeconds: 30,
      heartbeatIntervalInMs: 10_000,
      ...options,
    };

    this._consumerId = null;
    this._lease = null;
    this._heartbeatInterval = null;
    this._watchers = new Map();
    this._assignedQueues = new Set();
    this._queuePriorities = new Map();

    Coordinator.instance = this;
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
      this._consumerId = consumerId || this.#generateConsumerId();

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
      this.#startHeartbeat();

      // Watch for queue assignments
      await this.#watchQueueAssignments();

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
      this.#stopHeartbeat();

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

      // Default priority if not set
      return 1;
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
      // Get queue priorities
      const queuePriorities = {};
      let totalPriority = 0;

      for (const queue of queues) {
        const priority = await this.getQueuePriority(queue);
        queuePriorities[queue] = priority;
        totalPriority += priority;
      }

      // Calculate ideal distribution
      const consumerCount = consumers.length;
      const consumerAssignments = {};

      // Initialize consumer assignments
      for (const consumer of consumers) {
        consumerAssignments[consumer] = [];
      }

      // Handle case where there are more consumers than queues
      if (consumerCount >= queues.length) {
        // Calculate how many consumers each queue should get based on priority
        const queueConsumerCounts = {};
        let remainingConsumers = consumerCount;

        for (const queue of queues) {
          // Proportion of consumers for this queue based on priority
          const proportion = queuePriorities[queue] / totalPriority;
          // Initial distribution - give at least one consumer to each queue
          let assignedConsumers = Math.max(1, Math.floor(proportion * consumerCount));

          // Don't assign more consumers than are available
          assignedConsumers = Math.min(assignedConsumers, remainingConsumers);
          queueConsumerCounts[queue] = assignedConsumers;
          remainingConsumers -= assignedConsumers;
        }

        // Distribute any remaining consumers based on priority
        while (remainingConsumers > 0) {
          // Find queue with highest priority per consumer
          let bestQueue = null;
          let bestRatio = -1;

          for (const queue of queues) {
            const priorityPerConsumer = queuePriorities[queue] / queueConsumerCounts[queue];
            if (priorityPerConsumer > bestRatio) {
              bestRatio = priorityPerConsumer;
              bestQueue = queue;
            }
          }

          if (bestQueue) {
            queueConsumerCounts[bestQueue]++;
            remainingConsumers--;
          } else {
            break; // Shouldn't happen, but prevent infinite loop
          }
        }

        // Assign consumers to queues based on calculated distribution
        let consumerIndex = 0;
        for (const [queue, count] of Object.entries(queueConsumerCounts)) {
          for (let i = 0; i < count; i++) {
            const consumer = consumers[consumerIndex % consumerCount];
            consumerAssignments[consumer].push(queue);
            consumerIndex++;
          }
        }
      }
      // Handle case where there are more queues than consumers
      else {
        // Sort queues by priority (highest first)
        const sortedQueues = [...queues].sort((a, b) => queuePriorities[b] - queuePriorities[a]);

        // Group low-priority queues together when there are too many queues
        const highPriorityQueueCount = Math.min(consumerCount, sortedQueues.length);
        const highPriorityQueues = sortedQueues.slice(0, highPriorityQueueCount);
        const lowPriorityQueues = sortedQueues.slice(highPriorityQueueCount);

        // Assign one consumer to each high-priority queue
        for (let i = 0; i < highPriorityQueues.length; i++) {
          consumerAssignments[consumers[i]].push(highPriorityQueues[i]);
        }

        // Distribute remaining queues among consumers
        if (lowPriorityQueues.length > 0) {
          const chunkedQueues = this.#chunkArray(lowPriorityQueues, consumerCount);

          for (let i = 0; i < chunkedQueues.length; i++) {
            const consumerIndex = i % consumerCount;
            for (const queue of chunkedQueues[i]) {
              consumerAssignments[consumers[consumerIndex]].push(queue);
            }
          }
        }
      }

      return consumerAssignments;
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
   * Start heartbeat to keep consumer lease alive
   * @private
   */
  #startHeartbeat() {
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
  #stopHeartbeat() {
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval);
      this._heartbeatInterval = null;
    }
  }

  /**
   * Watch for queue assignment changes
   * @private
   */
  async #watchQueueAssignments() {
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
  #generateConsumerId() {
    const hostname = process.env.HOSTNAME || 'unknown';
    const timestamp = Date.now();
    return `CONSUMER-${hostname}-${timestamp}-${uuidv4()}`;
  }

  /**
   * Split an array into chunks
   * @param {Array} array - Array to split
   * @param {number} chunkCount - Number of chunks
   * @returns {Array<Array>} - Array of chunks
   * @private
   */
  #chunkArray(array, chunkCount) {
    const chunks = Array(chunkCount)
      .fill()
      .map(() => []);
    array.forEach((item, index) => {
      const chunkIndex = index % chunkCount;
      chunks[chunkIndex].push(item);
    });
    return chunks;
  }

  /**
   * Event handler for queue assignment changes
   * Override this method to handle assignment changes
   * @param {Array<string>} queues - New queue assignments
   */
  onAssignmentChanged(queues) {
    // This is a placeholder - should be overridden by QueueManager
    logger.debug(`Assignment changed event: ${queues.join(', ')}`);
  }
}

const coordinator = new Coordinator();
export default coordinator;
