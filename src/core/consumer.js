// consumer.js
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
      verifyFinalState: config.consumer.verifyFinalState,
      retryDelay: config.consumer.retryDelay,
      maxRetries: config.consumer.maxRetries,
      monitorInterval: config.consumer.monitorInterval,
    };

    this._stateHandlers = new Map();
    this._activeProcessing = new Map();
    this._processingCount = 0;
    this._successCount = 0;
    this._errorCount = 0;
    this._monitorInterval = null;

    logger.info('Consumer initialized with options:', this._options);
  }

  /**
   * Initialize the consumer
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      if (this._options.verifyFinalState) {
        await this._stateStorage.initializeTable();
      }

      // Register handlers with queue manager
      this._registerHandlersWithQueueManager();

      // Start monitoring
      this._startMonitoring();

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

    const stateKey = this._getStateTransitionKey(currentState, processingState, nextState);
    this._stateHandlers.set(stateKey, handler);

    // Register with queue manager if already initialized
    this._registerHandlerWithQueueManager(currentState, processingState, nextState);

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
    const stateKey = this._getStateTransitionKey(currentState, processingState, nextState);
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
   * This is the core message processing method that implements the state transition flow
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

    const stateKey = this._getStateTransitionKey(currentState, processingState, nextState);

    // Check if we have a handler for this state transition
    if (!this._stateHandlers.has(stateKey)) {
      logger.warn(`No handler found for state transition: ${stateKey}`);
      return false;
    }

    // Check if message is already being processed in Redis
    const isProcessing = await this._messageTracker.isProcessing(messageId, currentState);
    if (isProcessing) {
      logger.warn(
        `Message ${messageId} already being processed in state ${currentState}, skipping`
      );
      return false;
    }

    // Check if already completed in database (if verification enabled)
    if (this._options.verifyFinalState) {
      const finalState = await this._stateStorage.getFinalState(messageId, currentState);
      if (finalState) {
        logger.info(
          `Message ${messageId} already completed state transition to ${finalState.nextState}, skipping`
        );
        return true;
      }
    }

    // Mark as processing in Redis
    await this._messageTracker.createProcessingState(messageId, currentState, {
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

      // Delete processing state in Redis
      await this._messageTracker.deleteProcessingState(messageId, currentState);

      // Update success counter
      this._successCount++;

      logger.debug(
        `Successfully processed message ${messageId} with state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );
      return true;
    } catch (error) {
      // Update error counter
      this._errorCount++;

      // Delete processing state on error
      await this._messageTracker.deleteProcessingState(messageId, currentState);

      logger.error(`Error processing message ${messageId}:`, error);
      return false;
    } finally {
      // Clean up tracking
      this._processingCount--;
      this._activeProcessing.delete(processingKey);
    }
  }

  /**
   * Batch process multiple messages
   * @param {Array<Object>} messages - Array of message objects with content and metadata
   * @returns {Promise<Object>} - Map of messageId to success status
   */
  async batchProcessMessages(messages) {
    if (!messages || !messages.length) return {};

    const results = {};
    const processingInfos = [];

    try {
      // First check which messages we can process (not at capacity, have handlers, not already processing)
      const availableSlotsCount = this._options.maxConcurrent - this._processingCount;

      if (availableSlotsCount <= 0) {
        logger.warn(
          `Maximum concurrent processing limit (${this._options.maxConcurrent}) reached, delaying batch`
        );
        await new Promise(resolve => setTimeout(resolve, this._options.retryDelay));

        // After delay, check again how many slots we have
        const newAvailableSlotsCount = this._options.maxConcurrent - this._processingCount;
        if (newAvailableSlotsCount <= 0) {
          // Still no capacity
          for (const msg of messages) {
            results[msg.metadata.messageId] = false;
          }
          return results;
        }
      }

      // Process our batch allocation in steps for better clarity
      const messagesToProcess = await this._identifyMessagesToProcess(messages, results);
      const processingStatuses = await this._checkProcessingStatus(messagesToProcess);
      const readyMessages = await this._prepareMessagesForProcessing(
        messagesToProcess,
        processingStatuses,
        results,
        processingInfos
      );

      // Process the messages that are ready
      await this._executeMessageProcessing(readyMessages, processingInfos, results);

      return results;
    } catch (error) {
      logger.error('Error in batch message processing:', error);

      // Mark all as failed
      for (const info of processingInfos) {
        results[info.metadata.messageId] = false;
      }

      return results;
    }
  }

  /**
   * Identify which messages can be processed based on handlers
   * @param {Array<Object>} messages - Array of message objects
   * @param {Object} results - Results object to be updated
   * @returns {Array<Object>} - Filtered messages that can be processed
   * @private
   */
  async _identifyMessagesToProcess(messages, results) {
    const messagesToProcess = [];

    for (const msg of messages) {
      const { messageId, currentState, processingState, nextState } = msg.metadata;

      // Check if we have a handler for this state transition
      const stateKey = this._getStateTransitionKey(currentState, processingState, nextState);
      if (!this._stateHandlers.has(stateKey)) {
        logger.warn(`No handler found for state transition: ${stateKey}`);
        results[messageId] = false;
        continue;
      }

      messagesToProcess.push(msg);
    }

    return messagesToProcess;
  }

  /**
   * Check which messages are already being processed
   * @param {Array<Object>} messagesToProcess - Messages that can be processed
   * @returns {Object} - Map of messageId to processing status
   * @private
   */
  async _checkProcessingStatus(messagesToProcess) {
    const messageInfos = messagesToProcess.map(msg => ({
      messageId: msg.metadata.messageId,
      processingState: msg.metadata.currentState,
    }));

    return await this._messageTracker.batchIsProcessing(messageInfos);
  }

  /**
   * Prepare messages for processing by creating processing states
   * @param {Array<Object>} messagesToProcess - Messages that can be processed
   * @param {Object} processingStatuses - Map of messageId to processing status
   * @param {Object} results - Results object to be updated
   * @param {Array<Object>} processingInfos - Array to store processing info
   * @returns {Array<Object>} - Messages ready for processing
   * @private
   */
  async _prepareMessagesForProcessing(
    messagesToProcess,
    processingStatuses,
    results,
    processingInfos
  ) {
    const messagesToCreateState = [];

    for (const msg of messagesToProcess) {
      const { messageId, currentState } = msg.metadata;

      if (processingStatuses[messageId]) {
        logger.warn(
          `Message ${messageId} already being processed in state ${currentState}, skipping`
        );
        results[messageId] = false;
        continue;
      }

      messagesToCreateState.push({
        messageId,
        processingState: currentState,
        metadata: {
          processingState: msg.metadata.processingState,
          nextState: msg.metadata.nextState,
          startedAt: Date.now(),
        },
      });

      processingInfos.push({
        message: msg.content,
        metadata: msg.metadata,
      });
    }

    // Create processing states in batch
    const createResults =
      await this._messageTracker.batchCreateProcessingStates(messagesToCreateState);
    return { createResults, processingInfos };
  }

  /**
   * Execute message processing for the prepared messages
   * @param {Object} readyMessages - Object containing createResults and processingInfos
   * @param {Array<Object>} processingInfos - Array of processing info objects
   * @param {Object} results - Results object to be updated
   * @returns {Promise<void>}
   * @private
   */
  async _executeMessageProcessing(readyMessages, processingInfos, results) {
    const { createResults } = readyMessages;
    const processingPromises = [];

    for (const info of processingInfos) {
      const { messageId } = info.metadata;

      if (createResults[messageId]) {
        // Increment processing count
        this._processingCount++;

        // Add to active processing tracking
        const processingKey = `${messageId}:${info.metadata.currentState}`;
        this._activeProcessing.set(processingKey, {
          ...info.metadata,
          startedAt: Date.now(),
        });

        // Process the message
        processingPromises.push(
          this.processMessage(info.message, info.metadata)
            .then(success => {
              results[messageId] = success;
              if (success) this._successCount++;
              else this._errorCount++;
            })
            .catch(error => {
              logger.error(`Error in batch processing message ${messageId}:`, error);
              results[messageId] = false;
              this._errorCount++;
            })
            .finally(() => {
              // Clean up tracking
              this._processingCount--;
              this._activeProcessing.delete(processingKey);
            })
        );
      } else {
        results[messageId] = false;
      }
    }

    // Wait for all processing to complete
    await Promise.all(processingPromises);
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
   * Start consuming messages from all queues
   * @returns {Promise<void>}
   */
  async startConsuming() {
    try {
      // Get assigned queues from coordinator
      const queues = await this._queueManager._coordinator.getAssignedQueues();

      // Start consuming from all assigned queues
      for (const queue of queues) {
        await this._queueManager.startConsumingQueue(queue);
      }

      logger.info('Started consuming messages from assigned queues');
    } catch (error) {
      logger.error('Error starting message consumption:', error);
      throw error;
    }
  }

  /**
   * Stop consuming messages from all queues
   * @returns {Promise<void>}
   */
  async stopConsuming() {
    try {
      // Get active queues
      const activeQueues = this._queueManager.getActiveQueues();

      // If no active queues, log and return
      if (activeQueues.length === 0) {
        logger.info('No active queues to stop consuming from');
        return;
      }

      // Stop consuming from all active queues
      for (const queue of activeQueues) {
        await this._queueManager.stopConsumingQueue(queue);
      }

      logger.info('Stopped consuming messages from all queues');
    } catch (error) {
      logger.error('Error stopping message consumption:', error);
      throw error;
    }
  }

  /**
   * Register all handlers with the queue manager
   * @private
   */
  _registerHandlersWithQueueManager() {
    // Register each state handler with the queue manager
    for (const [stateKey, handler] of this._stateHandlers.entries()) {
      const [currentState, processingState, nextState] = stateKey.split(':');
      this._registerHandlerWithQueueManager(currentState, processingState, nextState);
    }
  }

  /**
   * Register a specific handler with the queue manager
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @private
   */
  _registerHandlerWithQueueManager(currentState, processingState, nextState) {
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
   * Implement message retry with exponential backoff
   * @param {string} messageId - Message ID
   * @param {string} currentState - Current state
   * @param {Function} retryFunction - Function to retry
   * @param {number} [initialDelayMs=1000] - Initial delay in milliseconds
   * @param {number} [maxAttempts=3] - Maximum retry attempts
   * @returns {Promise<any>} - Result of successful attempt or last error
   */
  async retryWithBackoff(
    messageId,
    currentState,
    retryFunction,
    initialDelayMs = 1000,
    maxAttempts = 3
  ) {
    let attempts = 0;
    let lastError = null;

    while (attempts < maxAttempts) {
      attempts++;
      try {
        return await retryFunction();
      } catch (error) {
        lastError = error;
        logger.warn(
          `Retry ${attempts}/${maxAttempts} failed for message ${messageId}: ${error.message}`
        );

        if (attempts < maxAttempts) {
          const delayMs = initialDelayMs * Math.pow(2, attempts - 1);
          logger.debug(`Waiting ${delayMs}ms before next retry for message ${messageId}`);
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
      }
    }

    throw lastError || new Error(`All ${maxAttempts} retry attempts failed`);
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
