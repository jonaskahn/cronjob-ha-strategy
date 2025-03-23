import getLogger from '../utils/logger.js';

const logger = getLogger('core/Consumer');

/**
 * Consumer class for processing messages with state transitions.
 * Registers state-specific handlers and coordinates with QueueManager
 * and MessageTracker for message processing.
 */
class Consumer {
  /**
   * Create a new Consumer instance
   * @param {Object} queueManager - QueueManager instance
   * @param {Object} messageTracker - MessageTracker instance
   * @param {Object} stateStorage - StateStorage instance
   * @param {Object} options - Configuration options
   */
  constructor(queueManager, messageTracker, stateStorage, options = {}) {
    if (Consumer.instance) {
      return Consumer.instance;
    }

    this._queueManager = queueManager;
    this._messageTracker = messageTracker;
    this._stateStorage = stateStorage;
    this._options = {
      maxConcurrent: 100, // Maximum concurrent messages
      verifyFinalState: true, // Verify final state in database
      retryDelay: 1000, // Retry delay in ms
      maxRetries: 3, // Maximum retry attempts
      monitorInterval: 60000, // Monitor interval in ms (1 minute)
      ...options,
    };

    this._stateHandlers = new Map();
    this._activeProcessing = new Map();
    this._processingCount = 0;
    this._successCount = 0;
    this._errorCount = 0;
    this._monitorInterval = null;

    Consumer.instance = this;

    logger.info('Consumer initialized with options:', this._options);

    // Register handlers with queue manager
    this.#registerHandlersWithQueueManager();
    // Start monitoring
    this.#startMonitoring();
  }

  /**
   * Initialize the consumer
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      // Verify state storage is initialized
      if (this._options.verifyFinalState) {
        await this._stateStorage.initializeTable();
      }

      logger.info('Consumer initialized successfully');
    } catch (error) {
      logger.error('Error initializing consumer:', error);
      throw error;
    }
  }

  /**
   * Register a handler for a specific state transition
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @param {Function} handler - Handler function
   * @returns {boolean} - Success indicator
   */
  registerStateHandler(currentState, processingState, nextState, handler) {
    if (!currentState || !processingState || !nextState) {
      throw new Error('State information (currentState, processingState, nextState) is required');
    }

    if (typeof handler !== 'function') {
      throw new Error('Handler must be a function');
    }

    const stateKey = this.#getStateTransitionKey(currentState, processingState, nextState);
    this._stateHandlers.set(stateKey, handler);

    // Register with queue manager if already initialized
    this.#registerHandlerWithQueueManager(currentState, processingState, nextState);

    logger.info(
      `Registered handler for state transition: ${currentState} -> ${processingState} -> ${nextState}`
    );
    return true;
  }

  /**
   * Unregister a state handler
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @returns {boolean} - Success indicator
   */
  unregisterStateHandler(currentState, processingState, nextState) {
    const stateKey = this.#getStateTransitionKey(currentState, processingState, nextState);
    const result = this._stateHandlers.delete(stateKey);

    if (result) {
      logger.info(
        `Unregistered handler for state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );
    } else {
      logger.warn(
        `No handler found for state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );
    }

    return result;
  }

  /**
   * Process a message with the appropriate state handler
   * @param {Object} message - Message content
   * @param {Object} metadata - Message metadata
   * @returns {Promise<boolean>} - Success indicator
   */
  async processMessage(message, metadata) {
    const { messageId, currentState, processingState, nextState } = metadata;

    if (!messageId || !currentState || !processingState || !nextState) {
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

    const stateKey = this.#getStateTransitionKey(currentState, processingState, nextState);

    // Check if we have a handler for this state transition
    if (!this._stateHandlers.has(stateKey)) {
      logger.warn(`No handler found for state transition: ${stateKey}`);
      return false;
    }

    // Track active processing
    this._processingCount++;
    const processingStart = Date.now();
    const processingKey = `${messageId}:${currentState}`;
    this._activeProcessing.set(processingKey, {
      messageId,
      currentState,
      processingState,
      nextState,
      startedAt: processingStart,
    });

    try {
      // Get the handler
      const handler = this._stateHandlers.get(stateKey);

      // Execute handler
      logger.debug(
        `Processing message ${messageId} with state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );
      await handler(message, metadata);

      // Store final state if requested
      if (this._options.verifyFinalState) {
        await this._stateStorage.storeFinalState(messageId, currentState, nextState, {
          completedAt: new Date(),
          metadata: JSON.stringify({
            processingTime: Date.now() - processingStart,
            ...metadata,
          }),
        });
      }

      // Update success counter
      this._successCount++;

      logger.debug(
        `Successfully processed message ${messageId} with state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );
      return true;
    } catch (error) {
      // Update error counter
      this._errorCount++;

      logger.error(`Error processing message ${messageId}:`, error);
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
    return {
      activeProcessing: this._processingCount,
      activeMessages: Array.from(this._activeProcessing.values()),
      totalSuccess: this._successCount,
      totalErrors: this._errorCount,
      successRate:
        this._successCount + this._errorCount > 0
          ? ((this._successCount / (this._successCount + this._errorCount)) * 100).toFixed(2) + '%'
          : '0%',
      registeredTransitions: Array.from(this._stateHandlers.keys()),
    };
  }

  /**
   * Reset statistics counters
   */
  resetStats() {
    this._successCount = 0;
    this._errorCount = 0;
    logger.info('Consumer statistics reset');
  }

  /**
   * Shutdown the consumer
   * @returns {Promise<void>}
   */
  async shutdown() {
    // Stop monitoring
    if (this._monitorInterval) {
      clearInterval(this._monitorInterval);
      this._monitorInterval = null;
    }

    // Wait for active processing to complete
    if (this._processingCount > 0) {
      logger.info(`Waiting for ${this._processingCount} active processes to complete`);

      // Wait up to 30 seconds for processing to complete
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
   * Register all handlers with the queue manager
   * @private
   */
  #registerHandlersWithQueueManager() {
    // Register each state handler with the queue manager
    for (const [stateKey, handler] of this._stateHandlers.entries()) {
      const [currentState, processingState, nextState] = stateKey.split(':');
      this.#registerHandlerWithQueueManager(currentState, processingState, nextState);
    }
  }

  /**
   * Register a specific handler with the queue manager
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @private
   */
  #registerHandlerWithQueueManager(currentState, processingState, nextState) {
    if (this._queueManager) {
      // Wrap the handler to use our processMessage method
      const wrappedHandler = async (message, metadata) => {
        return await this.processMessage(message, {
          ...metadata,
          currentState,
          processingState,
          nextState,
        });
      };

      this._queueManager.registerStateHandler(
        currentState,
        processingState,
        nextState,
        wrappedHandler
      );

      logger.debug(
        `Registered handler with QueueManager: ${currentState} -> ${processingState} -> ${nextState}`
      );
    }
  }

  /**
   * Start monitoring active processes
   * @private
   */
  #startMonitoring() {
    if (this._monitorInterval) {
      clearInterval(this._monitorInterval);
    }

    this._monitorInterval = setInterval(() => {
      this.#monitorActiveProcessing();
    }, this._options.monitorInterval);

    logger.debug(`Started monitoring interval (${this._options.monitorInterval}ms)`);
  }

  /**
   * Monitor active processing for stuck or long-running tasks
   * @private
   */
  #monitorActiveProcessing() {
    const now = Date.now();
    const longRunningThreshold = 5 * 60 * 1000; // 5 minutes

    let longRunningCount = 0;

    for (const [key, process] of this._activeProcessing.entries()) {
      const duration = now - process.startedAt;

      if (duration > longRunningThreshold) {
        longRunningCount++;
        logger.warn(`Long-running process detected: ${process.messageId} (${duration}ms)`);
      }
    }

    if (this._processingCount > 0) {
      logger.info(
        `Active processing: ${this._processingCount} messages (${longRunningCount} long-running)`
      );
    }

    // Log overall stats periodically
    logger.info(
      `Processing statistics: ${this._successCount} successful, ${this._errorCount} errors`
    );
  }

  /**
   * Get state transition key
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @returns {string} - State key
   * @private
   */
  #getStateTransitionKey(currentState, processingState, nextState) {
    return `${currentState}:${processingState}:${nextState}`;
  }
}

export default Consumer;
