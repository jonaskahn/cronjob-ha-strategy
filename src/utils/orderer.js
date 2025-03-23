// src/utils/priority-calculator.js
import getLogger from './logger.js';
import config from './config.js';

const logger = getLogger('utils/PriorityCalculator');

/**
 * PriorityCalculator handles priority-based resource allocation calculations
 * for distributing consumers across queues with different priority levels.
 */
class Orderer {
  constructor() {
    if (Orderer.instance) {
      return Orderer.instance;
    }

    this._config = config;
    this._priorityLevels = {
      high: this._config.get('priority.highPriority', 3),
      medium: this._config.get('priority.mediumPriority', 2),
      low: this._config.get('priority.lowPriority', 1),
    };

    this._priorityPatterns = this._config.get('priority.priorityQueuePatterns', {
      high: ['priority-3', 'high', 'urgent'],
      medium: ['priority-2', 'medium', 'normal'],
      low: ['priority-1', 'low', 'batch'],
    });

    Orderer.instance = this;

    logger.info('PriorityCalculator initialized');
  }

  /**
   * Determine the priority level of a queue based on its name
   * @param {string} queueName - Queue name
   * @returns {number} - Priority level (3=high, 2=medium, 1=low)
   */
  calculateQueuePriority(queueName) {
    // First check for explicit priority in the name (e.g., "priority-3")
    const priorityMatch = queueName.match(/priority-(\d+)/i);
    if (priorityMatch && priorityMatch[1]) {
      const explicitPriority = parseInt(priorityMatch[1], 10);
      if (explicitPriority > 0) {
        return Math.min(explicitPriority, 3); // Cap at 3
      }
    }

    // Check for priority patterns in the name
    const lowerQueueName = queueName.toLowerCase();

    // Check high priority patterns
    for (const pattern of this._priorityPatterns.high) {
      if (lowerQueueName.includes(pattern.toLowerCase())) {
        return this._priorityLevels.high;
      }
    }

    // Check medium priority patterns
    for (const pattern of this._priorityPatterns.medium) {
      if (lowerQueueName.includes(pattern.toLowerCase())) {
        return this._priorityLevels.medium;
      }
    }

    // Check low priority patterns
    for (const pattern of this._priorityPatterns.low) {
      if (lowerQueueName.includes(pattern.toLowerCase())) {
        return this._priorityLevels.low;
      }
    }

    // Default to medium priority if no patterns match
    return this._priorityLevels.medium;
  }

  /**
   * Calculate the optimal distribution of consumers to queues based on priorities
   * @param {Object} queuePriorities - Map of queue names to priority values
   * @param {number} consumerCount - Number of available consumers
   * @returns {Object} - Map of queue names to assigned consumer counts
   */
  calculateConsumerDistribution(queuePriorities, consumerCount) {
    // Calculate total priority weight
    let totalPriority = 0;
    for (const [queue, priority] of Object.entries(queuePriorities)) {
      totalPriority += priority;
    }

    // Initial distribution - allocate consumers proportionally to priority
    const idealDistribution = {};
    const queueNames = Object.keys(queuePriorities);
    let remainingConsumers = consumerCount;

    // First pass: ensure each queue gets at least one consumer
    for (const queue of queueNames) {
      idealDistribution[queue] = 1;
      remainingConsumers--;
    }

    // If we don't have enough consumers for each queue, allocate to highest priority first
    if (remainingConsumers < 0) {
      // Sort queues by priority (highest first)
      const sortedQueues = [...queueNames].sort((a, b) => queuePriorities[b] - queuePriorities[a]);

      // Reset distribution
      for (const queue of queueNames) {
        idealDistribution[queue] = 0;
      }

      // Allocate available consumers to highest priority queues
      for (let i = 0; i < Math.min(consumerCount, sortedQueues.length); i++) {
        idealDistribution[sortedQueues[i]] = 1;
      }

      logger.info(
        `Not enough consumers (${consumerCount}) for all queues (${queueNames.length}), allocated to highest priority queues`
      );
      return idealDistribution;
    }

    // Second pass: distribute remaining consumers proportionally to priority
    if (remainingConsumers > 0) {
      // Calculate priority-weighted distribution
      for (let i = 0; i < remainingConsumers; i++) {
        // Find the queue with the highest priority per current consumer
        let bestQueue = null;
        let bestRatio = -1;

        for (const queue of queueNames) {
          const priorityPerConsumer = queuePriorities[queue] / idealDistribution[queue];
          if (priorityPerConsumer > bestRatio) {
            bestRatio = priorityPerConsumer;
            bestQueue = queue;
          }
        }

        // Allocate one more consumer to the best queue
        if (bestQueue) {
          idealDistribution[bestQueue]++;
        }
      }
    }

    logger.debug(
      `Calculated consumer distribution for ${queueNames.length} queues with ${consumerCount} consumers:`,
      idealDistribution
    );
    return idealDistribution;
  }

  /**
   * Group low-priority queues when there are more queues than consumers
   * @param {Array<string>} queues - Queue names
   * @param {Object} queuePriorities - Map of queue names to priority values
   * @param {number} consumerCount - Number of available consumers
   * @returns {Object} - Map of consumer IDs to assigned queue arrays
   */
  groupLowPriorityQueues(queues, queuePriorities, consumerCount) {
    if (queues.length <= consumerCount) {
      // No need to group if we have enough consumers
      const consumerAssignments = {};
      for (let i = 0; i < consumerCount; i++) {
        consumerAssignments[`consumer-${i}`] = i < queues.length ? [queues[i]] : [];
      }
      return consumerAssignments;
    }

    // Sort queues by priority (highest first)
    const sortedQueues = [...queues].sort((a, b) => queuePriorities[b] - queuePriorities[a]);

    // Assign high-priority queues to dedicated consumers
    const highPriorityCount = Math.min(consumerCount, sortedQueues.length);
    const highPriorityQueues = sortedQueues.slice(0, highPriorityCount);
    const lowPriorityQueues = sortedQueues.slice(highPriorityCount);

    // Initialize consumer assignments
    const consumerAssignments = {};
    for (let i = 0; i < consumerCount; i++) {
      consumerAssignments[`consumer-${i}`] = [];
    }

    // Assign high-priority queues first (one per consumer)
    for (let i = 0; i < highPriorityQueues.length; i++) {
      consumerAssignments[`consumer-${i}`].push(highPriorityQueues[i]);
    }

    // Distribute low-priority queues among consumers
    // We'll use a simple round-robin approach
    for (let i = 0; i < lowPriorityQueues.length; i++) {
      const consumerIndex = i % consumerCount;
      consumerAssignments[`consumer-${consumerIndex}`].push(lowPriorityQueues[i]);
    }

    logger.debug(
      `Grouped ${lowPriorityQueues.length} low-priority queues among ${consumerCount} consumers`
    );
    return consumerAssignments;
  }

  /**
   * Calculate consumer allocation for multiple consumer IDs
   * @param {Array<string>} consumerIds - Consumer IDs
   * @param {Object} queuePriorities - Map of queue names to priority values
   * @returns {Object} - Map of consumer IDs to assigned queue arrays
   */
  calculateConsumerAllocation(consumerIds, queuePriorities) {
    const queueNames = Object.keys(queuePriorities);
    const consumerCount = consumerIds.length;

    // Get ideal distribution (queue name to consumer count)
    const distribution = this.calculateConsumerDistribution(queuePriorities, consumerCount);

    // Convert distribution to actual consumer assignments
    const consumerAssignments = {};
    let consumerIndex = 0;

    // Initialize all consumers with empty arrays
    for (const consumerId of consumerIds) {
      consumerAssignments[consumerId] = [];
    }

    // Assign queues to consumers based on calculated distribution
    for (const [queueName, count] of Object.entries(distribution)) {
      for (let i = 0; i < count; i++) {
        const consumerId = consumerIds[consumerIndex % consumerCount];
        consumerAssignments[consumerId].push(queueName);
        consumerIndex++;
      }
    }

    logger.info(`Allocated ${queueNames.length} queues to ${consumerCount} consumers`);
    return consumerAssignments;
  }
}

// Create singleton instance
const priorityCalculator = new Orderer();
export default priorityCalculator;
