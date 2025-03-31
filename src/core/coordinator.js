import getLogger from '../utils/logger.js';
import { v4 as uuidv4 } from 'uuid';
import * as os from 'os';

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
    this._consumerId = null;
    this._isConsumerRegistered = false;
    this._lease = null;
    this._heartbeatInterval = null;
    this._watchers = new Map();
    this._assignedQueues = new Set();
    this._queuePriorities = new Map();
    this._debugHeartbeats = false;
    this._lastRebalanceTime = 0;
    this._rebalanceCooldownMs = this._options.rebalanceCooldownMs || 30000;

    // Added: Store queue binding information
    this._queueBindings = new Map();

    logger.info('Coordinator initialized');
  }

  /**
   * Register this consumer with etcd
   * @param {string} [consumerId] - Optional consumer ID (generated if not provided)
   * @returns {Promise<string>} - Assigned consumer ID
   */
  async initialize(consumerId = null) {
    try {
      if (this._isConsumerRegistered) {
        return this._consumerId;
      }

      // Generate or use provided consumer ID
      this._consumerId = consumerId || this._generateConsumerId();

      // Create lease and register consumer
      this._lease = await this._etcdClient.createLease(this._options.leaseTTLInSeconds);
      const consumerKey = `${this._options.consumerPrefix}${this._consumerId}`;

      // Register with initial values - add more metadata to distinguish registration from heartbeat
      await this._etcdClient.set(
        consumerKey,
        JSON.stringify({
          registered: Date.now(),
          lastHeartbeat: Date.now(),
          hostname: os.hostname(),
          pid: process.pid,
          type: 'registration', // Explicitly mark this as registration
        }),
        this._lease
      );

      // Start watching assignments and registry
      await this._watchQueueAssignments();

      // Start heartbeat
      this._startHeartbeat();

      this._isConsumerRegistered = true;
      logger.info(`Registered consumer with ID: ${this._consumerId}`);

      // IMPORTANT: Force an initial assignment directly without using the watcher
      await this._forceInitialAssignment();

      // THEN set up the registry watcher for future changes
      await this._watchConsumerRegistry();

      return this._consumerId;
    } catch (error) {
      logger.error('Error registering consumer:', error);
      throw error;
    }
  }

  /**
   * Set the priority for a queue and register binding information
   * @param {string} queueName - Queue name
   * @param {number} priority - Priority value (higher = more important)
   * @param {Object} bindingInfo - Queue binding information
   * @param {string} bindingInfo.exchangeName - Exchange name to bind to
   * @param {string} bindingInfo.routingKey - Routing key for binding
   * @param {string} bindingInfo.exchangeType - Exchange type (direct, topic, fanout)
   * @returns {Promise<boolean>} - Success indicator
   */
  async setQueuePriority(queueName, priority, bindingInfo = null) {
    try {
      const queueConfigKey = `${this._options.queueConfigPrefix}${queueName}`;
      const config = {
        priority,
        updatedAt: Date.now(),
      };

      // Store binding information if provided
      if (bindingInfo) {
        config.binding = {
          exchangeName: bindingInfo.exchangeName,
          routingKey: bindingInfo.routingKey,
          exchangeType: bindingInfo.exchangeType || 'direct',
        };

        // Update local cache
        this._queueBindings.set(queueName, config.binding);
      }

      await this._etcdClient.set(queueConfigKey, JSON.stringify(config));
      this._queuePriorities.set(queueName, priority);

      logger.info(
        `Set queue ${queueName} priority to ${priority}${bindingInfo ? ' with binding information' : ''}`
      );
      return true;
    } catch (error) {
      logger.error(`Error setting priority for queue ${queueName}:`, error);
      return false;
    }
  }

  /**
   * Get binding information for a queue
   * @param {string} queueName - Queue name
   * @returns {Promise<Object|null>} - Binding information or null if not found
   */
  async getQueueBinding(queueName) {
    // Check local cache first
    if (this._queueBindings.has(queueName)) {
      return this._queueBindings.get(queueName);
    }

    try {
      const queueConfigKey = `${this._options.queueConfigPrefix}${queueName}`;
      const configJson = await this._etcdClient.get(queueConfigKey);

      if (configJson) {
        const config = JSON.parse(configJson);
        if (config.binding) {
          this._queueBindings.set(queueName, config.binding);
          return config.binding;
        }
      }

      return null;
    } catch (error) {
      logger.error(`Error getting binding for queue ${queueName}:`, error);
      return null;
    }
  }

  /**
   * Get all registered queue bindings
   * @returns {Promise<Object>} - Map of queue names to binding info
   */
  async getAllQueueBindings() {
    const bindings = {};

    try {
      const queueConfigs = await this._etcdClient.getWithPrefix(this._options.queueConfigPrefix);

      for (const [key, valueJson] of Object.entries(queueConfigs)) {
        const queueName = key.replace(this._options.queueConfigPrefix, '');
        const config = JSON.parse(valueJson);

        if (config.binding) {
          bindings[queueName] = config.binding;
          this._queueBindings.set(queueName, config.binding);
        }
      }

      return bindings;
    } catch (error) {
      logger.error('Error getting all queue bindings:', error);
      return {};
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

  // Other methods remain unchanged
  // ...

  /**
   * Force initial queue assignment when consumer starts up
   * @private
   */
  async _forceInitialAssignment() {
    try {
      logger.info('Performing forced initial assignment');

      // Get all registered queues
      const queuesWithPriorities = await this.getQueuesWithPriorities();
      const queues = Object.keys(queuesWithPriorities);

      if (queues.length === 0) {
        logger.info('No queues found for initial assignment');
        return;
      }

      // Get all consumers (including this one)
      const consumers = await this.getConsumers();
      logger.info(`Found ${consumers.length} consumers during initial assignment`);

      // Force a rebalance
      const distribution = await this.calculateConsumerDistribution(queues, consumers);
      await this.applyConsumerDistribution(distribution);

      // Update last rebalance time
      this._lastRebalanceTime = Date.now();

      logger.info(
        `Initial assignment complete with ${consumers.length} consumers, ${queues.length} queues`
      );

      // Log this consumer's assignments
      const myQueues = distribution[this._consumerId] || [];
      if (myQueues.length > 0) {
        logger.info(`This consumer was assigned queues: ${myQueues.join(', ')}`);
      } else {
        logger.warn('This consumer was not assigned any queues during initial assignment');
      }
    } catch (error) {
      logger.error('Error performing initial assignment:', error);
    }
  }

  /**
   * Deregister this consumer from etcd
   * @returns {Promise<boolean>} - Success indicator
   */
  async shutdown() {
    if (!this._consumerId) {
      logger.warn('Cannot deregister: Consumer not registered');
      return false;
    }

    try {
      this._stopHeartbeat();
      if (this._lease) {
        await this._lease.revoke();
        this._lease = null;
      }

      const consumerKey = `${this._options.consumerPrefix}${this._consumerId}`;
      await this._etcdClient.delete(consumerKey);

      // Clean up heartbeat key as well
      const heartbeatKey = `${this._options.consumerPrefix}heartbeats/${this._consumerId}`;
      await this._etcdClient.delete(heartbeatKey);

      // Clean up queue assignments in etcd
      const assignmentKey = `${this._options.queueAssignmentPrefix}${this._consumerId}`;
      await this._etcdClient.delete(assignmentKey);

      for (const watcher of this._watchers.values()) {
        watcher.cancel();
      }
      this._watchers.clear();

      // Clear assignments
      this._assignedQueues.clear();

      // Reset registration flag
      this._isConsumerRegistered = false;

      logger.info(`Deregistered consumer ${this._consumerId}`);
      return true;
    } catch (error) {
      logger.error(`Error deregistering consumer ${this._consumerId}:`, error);
      return false;
    }
  }

  /**
   * Get all registered consumers
   * @returns {Promise<Array<string>>} - Array of consumer IDs
   */
  async getConsumers() {
    try {
      const consumers = await this._etcdClient.getWithPrefix(this._options.consumerPrefix);
      // Filter out heartbeat keys
      return Object.keys(consumers)
        .filter(key => !key.includes('/heartbeats/'))
        .map(key => key.replace(this._options.consumerPrefix, ''));
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

        // Update bindings cache if present
        if (config.binding) {
          this._queueBindings.set(queueName, config.binding);
        }
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
      logger.info(
        `Calculating distribution for ${consumers.length} consumers and ${queues.length} queues`
      );

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
        logger.info(
          `More consumers (${consumers.length}) than queues (${queues.length}) - distributing consumers to queues`
        );
        return this._distributeConsumersToQueues(
          sortedQueues,
          consumers,
          queuePriorities,
          consumerAssignments
        );
      }
      // Case 2: More queues than consumers - distribute queues evenly
      else {
        logger.info(
          `More queues (${queues.length}) than consumers (${consumers.length}) - distributing queues to consumers`
        );
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
        logger.debug(`Assigned consumer ${consumerId} to queues: ${queues.join(', ')}`);
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
      logger.info(`Rebalancing ${consumers.length} consumers across ${queues.length} queues`);

      if (consumers.length === 0) {
        logger.warn('No consumers available for rebalancing');
        return false;
      }

      // Calculate optimal distribution
      const distribution = await this.calculateConsumerDistribution(queues, consumers);

      // Apply the distribution
      await this.applyConsumerDistribution(distribution);

      logger.info(
        `Rebalancing complete: ${consumers.length} consumers across ${queues.length} queues`
      );
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
   * Watch for changes in the consumer registry
   * @private
   */
  async _watchConsumerRegistry() {
    try {
      const consumerPrefix = this._options.consumerPrefix;

      // Get initial state of the consumer registry to track changes properly
      const initialRegistry = await this._etcdClient.getWithPrefix(consumerPrefix);
      const knownConsumers = new Set(
        Object.keys(initialRegistry)
          .filter(key => !key.includes('/heartbeats/')) // Filter out heartbeat keys
          .map(key => key.replace(consumerPrefix, ''))
      );

      logger.info(
        `Initial consumer registry state (${knownConsumers.size} consumers): ${Array.from(knownConsumers).join(', ')}`
      );

      const watcher = await this._etcdClient.watchPrefix(
        consumerPrefix,
        async (event, key, value) => {
          // Skip heartbeat keys completely
          if (key.includes('/heartbeats/')) {
            return;
          }

          const consumerId = key.replace(consumerPrefix, '');

          if (event === 'put') {
            let isHeartbeat = false;
            try {
              const data = JSON.parse(value);
              // More robust heartbeat detection
              isHeartbeat =
                (data &&
                  data.lastHeartbeat &&
                  data.registered &&
                  data.lastHeartbeat > data.registered) ||
                (data && data.type === 'heartbeat');
            } catch (e) {
              logger.warn(`Failed to parse consumer data for ${consumerId}: ${e.message}`);
            }

            if (isHeartbeat) {
              // Skip heartbeat updates silently - this is the critical fix
              if (this._debugHeartbeats) {
                logger.debug(`Ignoring heartbeat update for consumer ${consumerId}`);
              }
              return;
            }

            // If this is our own registration, just add to known consumers
            if (consumerId === this._consumerId) {
              knownConsumers.add(consumerId);
              logger.debug(`Added self (${consumerId}) to known consumers`);
              return;
            }

            // If this is a new consumer we haven't seen before
            if (!knownConsumers.has(consumerId)) {
              logger.info(`NEW CONSUMER DETECTED: ${consumerId}`);
              knownConsumers.add(consumerId);

              // Check cooldown period
              const timeSinceLastRebalance = Date.now() - this._lastRebalanceTime;
              if (timeSinceLastRebalance < this._rebalanceCooldownMs) {
                logger.info(
                  `Waiting for cooldown period (${timeSinceLastRebalance}ms / ${this._rebalanceCooldownMs}ms)`
                );
                return;
              }

              // Trigger rebalance
              logger.info(`Triggering rebalance due to new consumer ${consumerId}`);
              await this._triggerRebalance();
            } else {
              logger.debug(`Received update for existing consumer ${consumerId}`);
            }
          }
          // Handle consumer leaving
          else if (event === 'delete') {
            if (knownConsumers.has(consumerId)) {
              logger.info(`CONSUMER DELETED: ${consumerId}`);
              knownConsumers.delete(consumerId);

              // Always rebalance when a consumer leaves
              logger.info(`Triggering rebalance due to consumer ${consumerId} leaving`);
              await this._triggerRebalance();
            } else {
              logger.debug(`Received delete event for unknown consumer ${consumerId}`);
            }
          }
        }
      );

      this._watchers.set('consumerRegistry', watcher);
      logger.info('Started watching consumer registry for changes');
    } catch (error) {
      logger.error('Error watching consumer registry:', error);
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

      const watcher = await this._etcdClient.watchPrefix(
        assignmentKey,
        async (event, key, value) => {
          if (event === 'put') {
            const queues = JSON.parse(value);
            this._assignedQueues = new Set(queues);
            logger.info(`Queue assignments updated for consumer ${this._consumerId}:`, queues);
            this.onAssignmentChanged(queues);
          } else if (event === 'delete') {
            this._assignedQueues.clear();
            logger.info(`Queue assignments removed for consumer ${this._consumerId}`);
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
   * Trigger rebalancing operation
   * @private
   */
  async _triggerRebalance() {
    try {
      // Get all active consumers
      const consumers = await this.getConsumers();
      if (consumers.length === 0) {
        logger.warn('No consumers available for rebalancing');
        return;
      }

      logger.info(`Current consumers for rebalancing: ${consumers.join(', ')}`);

      // Simple leader election - oldest consumer performs rebalancing
      consumers.sort();
      const leaderConsumerId = consumers[0];

      logger.info(
        `Leader election result: ${leaderConsumerId} (self: ${this._consumerId}, isLeader: ${leaderConsumerId === this._consumerId})`
      );

      if (leaderConsumerId === this._consumerId) {
        logger.info('This coordinator is the leader, performing rebalancing');

        // Get all registered queues to rebalance
        const queuesWithPriorities = await this.getQueuesWithPriorities();
        const queues = Object.keys(queuesWithPriorities);

        if (queues.length === 0) {
          logger.warn('No queues found for rebalancing');
          return;
        }

        logger.info(`Rebalancing ${queues.length} queues: ${queues.join(', ')}`);

        // Perform rebalance
        await this.rebalanceConsumers(queues);

        // Update last rebalance time
        this._lastRebalanceTime = Date.now();
        logger.info('Rebalancing completed successfully');
      } else {
        logger.info(
          `Consumer ${leaderConsumerId} is the leader for rebalancing, not taking action`
        );

        this._lastRebalanceTime = Date.now();
      }
    } catch (error) {
      logger.error('Error triggering rebalance:', error);
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
   * Start heartbeat to keep consumer lease alive
   * @private
   */
  _startHeartbeat() {
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval);
    }

    // Set heartbeat interval to half of the lease TTL to ensure timely renewal
    const heartbeatInterval = Math.floor((this._options.leaseTTLInSeconds * 1000) / 2);

    this._heartbeatInterval = setInterval(async () => {
      try {
        // Keep lease alive
        await this._lease.keepalive();

        // Use a separate key for heartbeats to avoid triggering the watcher
        const heartbeatKey = `${this._options.consumerPrefix}heartbeats/${this._consumerId}`;
        await this._etcdClient.set(
          heartbeatKey,
          JSON.stringify({
            lastHeartbeat: Date.now(),
            type: 'heartbeat',
          }),
          this._lease
        );

        // Only log at trace level to reduce noise
        if (this._debugHeartbeats) {
          logger.debug(`Sent heartbeat for consumer ${this._consumerId}`);
        }
      } catch (error) {
        logger.error(`Error sending heartbeat for consumer ${this._consumerId}:`, error);
      }
    }, heartbeatInterval);
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
   * Generate a unique consumer ID
   * @returns {string} - Generated consumer ID
   * @private
   */
  _generateConsumerId() {
    const hostname = process.env.HOSTNAME || os.hostname() || 'unknown';
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
