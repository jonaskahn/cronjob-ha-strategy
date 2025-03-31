import getLogger from '../utils/logger.js';

const logger = getLogger('core/Consumer');

/**
 * Consumer class for processing messages with state transitions.
 * Registers state-specific handlers and coordinates with QueueManager
 * and MessageTracker for message processing.
 */
export default class Consumer {
  /**
   * Create a new Consumer instance
   * @param {QueueManager} queueManager - QueueManager instance
   * @param {MessageTracker} messageTracker - MessageTracker instance
   * @param {AppConfig} config - System configuration
   */
  constructor(queueManager, messageTracker, config) {
    this._queueManager = queueManager;
    this._messageTracker = messageTracker;
    this._options = {
      maxConcurrent: config.consumer.maxConcurrent,
      retryDelay: config.consumer.retryDelay,
      maxRetries: config.consumer.maxRetries,
      monitorInterval: config.consumer.monitorInterval,
    };

    this._stateHandlers = new Map();
    this._queueCallbacks = new Map(); // Store callbacks by queue
    this._activeProcessing = new Map();
    this._processingCount = 0;
    this._successCount = 0;
    this._errorCount = 0;
    this._monitorInterval = null;
    this._queueStats = new Map(); // Track stats by queue

    logger.info('Consumer initialized with options:', this._options);
  }

  /**
   * Initialize the consumer
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      this._startMonitoring();
      logger.info('Consumer initialized successfully');
    } catch (error) {
      logger.error('Error initializing consumer:', error);
      throw error;
    }
  }

  /**
   * Register state handler and callbacks in a single method
   * @param {Object} options - Registration options
   * @param {string} options.queueName - Queue name
   * @param {string} options.currentState - Current state
   * @param {string} options.processingState - Processing state
   * @param {string} options.nextState - Next state
   * @param {Function} options.handler - Message processing handler function
   * @param {Function} [options.isCompletedProcess] - Function to check if process already completed
   * @param {Function} [options.onCompletedProcess] - Function to call on successful processing
   * @param {Function} [options.onErrorProcess] - Function to call on processing error
   * @param {Object} [options.bindingInfo] - Queue binding information
   * @param {string} options.bindingInfo.exchangeName - Exchange name
   * @param {string} options.bindingInfo.routingKey - Routing key
   * @param {string} [options.bindingInfo.exchangeType] - Exchange type (direct, topic, fanout)
   * @param {number} [options.priority=1] - Queue priority (1=low, 2=medium, 3=high)
   * @returns {boolean} - Success indicator
   */
  registerHandler(options) {
    const {
      queueName,
      currentState,
      processingState,
      nextState,
      handler,
      isCompletedProcess,
      onCompletedProcess,
      onErrorProcess,
      bindingInfo,
      priority = 1,
    } = options;

    if (!queueName || !currentState || !processingState || !nextState) {
      throw new Error('Queue name and state information are required');
    }

    if (typeof handler !== 'function') {
      throw new Error('Handler must be a function');
    }

    // Initialize queue stats if not exists
    if (!this._queueStats.has(queueName)) {
      this._queueStats.set(queueName, {
        processCount: 0,
        successCount: 0,
        errorCount: 0,
      });
    }

    // Register callbacks for this queue
    this._queueCallbacks.set(queueName, {
      isCompletedProcess: isCompletedProcess || (async () => null),
      onCompletedProcess: onCompletedProcess || (async () => true),
      onErrorProcess: onErrorProcess || (async () => true),
    });

    // Register state handler
    const stateKey = `${queueName}:${this._getStateTransitionKey(currentState, processingState, nextState)}`;
    this._stateHandlers.set(stateKey, {
      handler,
      queueName,
      currentState,
      processingState,
      nextState,
    });

    // Register with queue manager - including binding information
    this._registerHandlerWithQueueManager(
      queueName,
      currentState,
      processingState,
      nextState,
      bindingInfo,
      priority
    );

    logger.info(
      `Registered handler for queue ${queueName} state transition: ${currentState} -> ${processingState} -> ${nextState}${bindingInfo ? ' with binding information' : ''}`
    );
    return true;
  }

  /**
   * Process a message with the appropriate state handler
   * @param {Object} message - Message content
   * @param {Object} metadata - Message metadata
   * @returns {Promise<boolean>} - Success indicator
   */
  async processMessage(message, metadata) {
    const { messageId, queueName, currentState, processingState, nextState } = metadata;

    if (!messageId || !queueName || !currentState || !processingState || !nextState) {
      logger.error('Missing required metadata for message processing');
      return false;
    }

    // Check if we're at max concurrent processing
    if (this._processingCount >= this._options.maxConcurrent) {
      logger.warn(
        `Maximum concurrent processing limit (${this._options.maxConcurrent}) reached, delaying processing`
      );
      await new Promise(resolve => setTimeout(resolve, this._options.retryDelay));
    }

    // Get queue-specific state key
    const stateKey = `${queueName}:${this._getStateTransitionKey(currentState, processingState, nextState)}`;

    // Check if we have a handler for this state transition
    if (!this._stateHandlers.has(stateKey)) {
      logger.warn(`No handler found for state transition: ${stateKey}`);
      return false;
    }

    // Get queue-specific callbacks or use defaults
    const callbacks = this._queueCallbacks.get(queueName) || {
      isCompletedProcess: async () => null,
      onCompletedProcess: async () => true,
      onErrorProcess: async () => true,
    };

    // Get queue stats
    const queueStats = this._queueStats.get(queueName) || {
      processCount: 0,
      successCount: 0,
      errorCount: 0,
    };
    queueStats.processCount++;
    this._queueStats.set(queueName, queueStats);

    // Check if message is already being processed in Redis
    const isProcessing = await this._messageTracker.isProcessing(messageId, currentState);
    if (isProcessing) {
      logger.warn(
        `Message ${messageId} already being processed in state ${currentState}, skipping`
      );
      return false;
    }

    // Check if already completed using the queue-specific callback
    const finalState = await callbacks.isCompletedProcess(messageId, currentState);
    if (finalState) {
      logger.info(
        `Message ${messageId} from queue ${queueName} already completed state transition, skipping`
      );
      return true;
    }

    // Mark as processing in Redis
    await this._messageTracker.createProcessingState(messageId, currentState, {
      queueName,
      processingState,
      nextState,
      startedAt: Date.now(),
    });

    // Track active processing
    this._processingCount++;
    const processingStart = Date.now();
    const processingKey = `${messageId}:${currentState}`;
    this._activeProcessing.set(processingKey, {
      messageId,
      queueName,
      currentState,
      processingState,
      nextState,
      startedAt: processingStart,
    });

    try {
      // Get the handler
      const handlerInfo = this._stateHandlers.get(stateKey);
      const handler = handlerInfo.handler;

      // Execute handler
      logger.debug(
        `Processing message ${messageId} from queue ${queueName} with state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );
      await handler(message, metadata);

      // Call queue-specific success callback
      await callbacks.onCompletedProcess(messageId, currentState, nextState, {
        completedAt: new Date(),
        processingTime: Date.now() - processingStart,
        ...metadata,
      });

      // Delete processing state in Redis
      await this._messageTracker.deleteProcessingState(messageId, currentState);

      // Update success counters
      this._successCount++;
      queueStats.successCount++;
      this._queueStats.set(queueName, queueStats);

      logger.debug(
        `Successfully processed message ${messageId} from queue ${queueName} with state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );
      return true;
    } catch (error) {
      // Update error counters
      this._errorCount++;
      queueStats.errorCount++;
      this._queueStats.set(queueName, queueStats);

      // Call queue-specific error callback
      await callbacks.onErrorProcess(messageId, currentState, error, {
        error: error.message,
        stack: error.stack,
        failedAt: new Date(),
        processingTime: Date.now() - processingStart,
        ...metadata,
      });

      // Delete processing state on error
      await this._messageTracker.deleteProcessingState(messageId, currentState);

      logger.error(`Error processing message ${messageId} from queue ${queueName}:`, error);
      return false;
    } finally {
      // Clean up tracking
      this._processingCount--;
      this._activeProcessing.delete(processingKey);
    }
  }

  /**
   * Get statistics about message processing
   * @returns {Object} - Statistics object
   */
  getStats() {
    // Create queue-specific stats
    const queueStatsObj = {};
    for (const [queueName, stats] of this._queueStats.entries()) {
      const activeCount = Array.from(this._activeProcessing.values()).filter(
        info => info.queueName === queueName
      ).length;

      queueStatsObj[queueName] = {
        ...stats,
        activeProcessing: activeCount,
        successRate:
          stats.processCount > 0
            ? ((stats.successCount / stats.processCount) * 100).toFixed(2) + '%'
            : '0%',
      };
    }

    return {
      totalActiveProcessing: this._processingCount,
      totalSuccess: this._successCount,
      totalErrors: this._errorCount,
      successRate:
        this._successCount + this._errorCount > 0
          ? ((this._successCount / (this._successCount + this._errorCount)) * 100).toFixed(2) + '%'
          : '0%',
      queueStats: queueStatsObj,
      activeMessages: Array.from(this._activeProcessing.values()),
    };
  }

  /**
   * Reset statistics counters
   */
  resetStats() {
    this._successCount = 0;
    this._errorCount = 0;

    // Reset queue-specific stats
    for (const [queueName, stats] of this._queueStats.entries()) {
      this._queueStats.set(queueName, {
        processCount: 0,
        successCount: 0,
        errorCount: 0,
      });
    }

    logger.info('Consumer statistics reset');
  }

  /**
   * Shutdown the consumer
   * @returns {Promise<void>}
   */
  async shutdown() {
    if (this._monitorInterval) {
      clearInterval(this._monitorInterval);
      this._monitorInterval = null;
    }
    if (this._processingCount > 0) {
      logger.info(`Waiting for ${this._processingCount} active processes to complete`);
      const maxWaitTime = 30000;
      const startTime = Date.now();

      while (this._processingCount > 0 && Date.now() - startTime < maxWaitTime) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }

      if (this._processingCount > 0) {
        logger.warn(`Shutdown complete with ${this._processingCount} messages still processing`);
      } else {
        logger.info('All processing completed before shutdown');
      }
    }

    logger.info('Consumer shutdown complete');
  }

  /**
   * Register a specific handler with the queue manager
   * @param {string} queueName - Queue name
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @param {Object} [bindingInfo] - Optional queue binding information
   * @param {number} [priority=1] - Queue priority
   * @private
   */
  _registerHandlerWithQueueManager(
    queueName,
    currentState,
    processingState,
    nextState,
    bindingInfo = null,
    priority = 1
  ) {
    if (this._queueManager) {
      // Get the state key
      const stateKey = `${queueName}:${this._getStateTransitionKey(currentState, processingState, nextState)}`;

      // Get the handler
      const handlerInfo = this._stateHandlers.get(stateKey);
      if (!handlerInfo) {
        logger.warn(`No handler found for registration: ${stateKey}`);
        return;
      }

      // Wrap the handler to use our processMessage method
      const wrappedHandler = async (message, metadata) => {
        return await this.processMessage(message, {
          ...metadata,
          queueName,
          currentState,
          processingState,
          nextState,
        });
      };

      this._queueManager.registerQueueStateHandler(
        queueName,
        currentState,
        processingState,
        nextState,
        wrappedHandler,
        bindingInfo,
        priority
      );

      logger.debug(
        `Registered handler with QueueManager for queue ${queueName}: ${currentState} -> ${processingState} -> ${nextState}`
      );
    }
  }

  /**
   * Start monitoring active processes
   * @private
   */
  _startMonitoring() {
    if (this._monitorInterval) {
      clearInterval(this._monitorInterval);
    }

    this._monitorInterval = setInterval(() => {
      this._monitorActiveProcessing();
    }, this._options.monitorInterval);

    logger.debug(`Started monitoring interval (${this._options.monitorInterval}ms)`);
  }

  /**
   * Monitor active processing for stuck or long-running tasks
   * @private
   */
  _monitorActiveProcessing() {
    const now = Date.now();
    const longRunningThreshold = 5 * 60 * 1000; // 5 minutes

    // Group by queue
    const queueCounts = {};
    const longRunningByQueue = {};

    for (const [key, process] of this._activeProcessing.entries()) {
      const { queueName } = process;
      const duration = now - process.startedAt;

      // Initialize queue counters
      if (!queueCounts[queueName]) {
        queueCounts[queueName] = 0;
        longRunningByQueue[queueName] = 0;
      }

      // Increment counters
      queueCounts[queueName]++;

      if (duration > longRunningThreshold) {
        longRunningByQueue[queueName]++;
        logger.warn(
          `Long-running process detected in queue ${queueName}: ${process.messageId} (${duration}ms)`
        );
      }
    }

    // Log queue statistics
    for (const [queueName, count] of Object.entries(queueCounts)) {
      if (count > 0) {
        logger.info(
          `Queue ${queueName}: ${count} active messages (${longRunningByQueue[queueName]} long-running)`
        );
      }
    }

    // Log overall stats periodically
    const queueStats = this.getStats().queueStats;
    for (const [queueName, stats] of Object.entries(queueStats)) {
      logger.info(
        `Queue ${queueName} statistics: ${stats.successCount} successful, ${stats.errorCount} errors, ${stats.successRate} success rate`
      );
    }
  }

  /**
   * Get state transition key
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @returns {string} - State key
   * @private
   */
  _getStateTransitionKey(currentState, processingState, nextState) {
    return `${currentState}:${processingState}:${nextState}`;
  }
}
