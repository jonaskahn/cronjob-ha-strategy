import getLogger from '../utils/logger.js';

const logger = getLogger('core/QueueManager');

/**
 * QueueManager handles RabbitMQ queue operations and integrates with
 * Coordinator for dynamic consumer-queue assignment based on priorities.
 */
export default class QueueManager {
  /**
   * Create a new QueueManager instance
   * @param {RabbitMQClient} rabbitMQClient - RabbitMQ client instance
   * @param {Coordinator} coordinator - Coordinator instance
   * @param {MessageTracker} messageTracker - MessageTracker instance
   * @param {AppConfig} config - System configuration
   */
  constructor(rabbitMQClient, coordinator, messageTracker, config) {
    this._rabbitMQClient = rabbitMQClient;
    this._coordinator = coordinator;
    this._messageTracker = messageTracker;
    this._options = {
      prefetch: config.rabbitmq.prefetch,
      defaultQueueOptions: config.rabbitmq.defaultQueueOptions,
      defaultExchangeOptions: config.rabbitmq.defaultExchangeOptions,
    };

    this._stateHandlers = new Map();
    this._activeConsumers = new Map();
    this._declaredQueues = new Set();

    this._coordinator.onAssignmentChanged = this._handleAssignmentChange.bind(this);
    logger.info('QueueManager initialized');
  }

  /**
   * Initialize the QueueManager and start processing assigned queues
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      await this._coordinator.register();
      const assignedQueues = await this._coordinator.getAssignedQueues();

      if (assignedQueues.length > 0) {
        logger.info(`Starting consumption for ${assignedQueues.length} assigned queues`);
        for (const queueName of assignedQueues) {
          await this.startConsumingQueue(queueName);
        }
      } else {
        logger.info('No queues assigned initially');
      }
      logger.info('QueueManager initialized successfully');
    } catch (error) {
      logger.error('Error initializing QueueManager:', error);
      throw error;
    }
  }

  /**
   * Register a handler for a specific state transition
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next (final) state
   * @param {Function} handler - Handler function (message, metadata) => Promise<void>
   */
  registerStateHandler(currentState, processingState, nextState, handler) {
    const stateKey = this._getStateTransitionKey(currentState, processingState, nextState);
    this._stateHandlers.set(stateKey, handler);

    logger.info(
      `Registered handler for state transition: ${currentState} -> ${processingState} -> ${nextState}`
    );
  }

  /**
   * Start consuming messages from a queue
   * @param {string} queueName - Queue name
   * @returns {Promise<string>} - Consumer tag
   */
  async startConsumingQueue(queueName) {
    try {
      if (this._activeConsumers.has(queueName)) {
        logger.debug(`Already consuming from queue ${queueName}`);
        return this._activeConsumers.get(queueName);
      }

      // Check if queue is assigned to this consumer
      const isAssigned = await this._coordinator.isQueueAssigned(queueName);
      if (!isAssigned) {
        logger.warn(`Queue ${queueName} is not assigned to this consumer, skipping`);
        return null;
      }

      // Ensure queue exists
      if (!this._declaredQueues.has(queueName)) {
        await this._rabbitMQClient.assertQueue(queueName, this._options.defaultQueueOptions);
        this._declaredQueues.add(queueName);
      }

      // Start consuming from the queue
      const consumerTag = await this._rabbitMQClient.consume(
        queueName,
        message => this._processMessage(message, queueName),
        { prefetch: this._options.prefetch }
      );

      // Track active consumer
      this._activeConsumers.set(queueName, consumerTag);
      logger.info(`Started consuming from queue ${queueName} with tag ${consumerTag}`);
      return consumerTag;
    } catch (error) {
      logger.error(`Error starting consumption from queue ${queueName}:`, error);
      throw error;
    }
  }

  /**
   * Stop consuming messages from a queue
   * @param {string} queueName - Queue name
   * @returns {Promise<boolean>} - Success indicator
   */
  async stopConsumingQueue(queueName) {
    try {
      // Check if consuming
      if (!this._activeConsumers.has(queueName)) {
        logger.debug(`Not consuming from queue ${queueName}, nothing to stop`);
        return true;
      }

      // Cancel consumer
      await this._rabbitMQClient.cancelConsumer(queueName);

      // Remove from active consumers
      this._activeConsumers.delete(queueName);

      logger.info(`Stopped consuming from queue ${queueName}`);
      return true;
    } catch (error) {
      logger.error(`Error stopping consumption from queue ${queueName}:`, error);
      return false;
    }
  }

  /**
   * Get all active queues being consumed
   * @returns {Array<string>} - Array of queue names
   */
  getActiveQueues() {
    return Array.from(this._activeConsumers.keys());
  }

  /**
   * Shutdown QueueManager and clean up resources
   * @returns {Promise<void>}
   */
  async shutdown() {
    try {
      // Stop all consumers
      const activeQueues = this.getActiveQueues();
      for (const queueName of activeQueues) {
        await this.stopConsumingQueue(queueName);
      }

      // Deregister from coordinator
      await this._coordinator.deregister();

      logger.info('QueueManager shutdown complete');
    } catch (error) {
      logger.error('Error during QueueManager shutdown:', error);
      throw error;
    }
  }

  /**
   * Handle queue assignment changes from coordinator
   * @param {Array<string>} newQueues - New queue assignments
   * @private
   */
  async _handleAssignmentChange(newQueues) {
    try {
      // Get current active queues
      const currentQueues = this.getActiveQueues();

      // Find queues to stop consuming
      const queuesToStop = currentQueues.filter(q => !newQueues.includes(q));

      // Find queues to start consuming
      const queuesToStart = newQueues.filter(q => !currentQueues.includes(q));

      // Stop consuming from removed queues
      for (const queueName of queuesToStop) {
        await this.stopConsumingQueue(queueName);
      }

      // Start consuming from new queues
      for (const queueName of queuesToStart) {
        await this.startConsumingQueue(queueName);
      }

      logger.info(
        `Queue assignments updated: Stopped ${queuesToStop.length}, Started ${queuesToStart.length}`
      );
    } catch (error) {
      logger.error('Error handling assignment change:', error);
    }
  }

  /**
   * Process a message received from RabbitMQ
   * @param {Object} message - RabbitMQ message
   * @param {string} queueName - Queue name
   * @private
   */
  async _processMessage(message, queueName) {
    // Extract message ID and state information
    const headers = message.properties.headers || {};
    const messageId = headers['x-message-id'] || message.properties.messageId;
    const currentState = headers['x-current-state'];
    const processingState = headers['x-processing-state'];
    const nextState = headers['x-next-state'];

    // Validate message has required information
    if (
      !this._validateMessageHeaders(
        message,
        messageId,
        currentState,
        processingState,
        nextState,
        queueName
      )
    ) {
      return;
    }

    try {
      // Process the message through multiple stages
      if (await this._isMessageAlreadyProcessing(messageId, currentState)) {
        await message.ack(); // Acknowledge to remove from queue
        return;
      }

      // Create processing state in Redis
      await this._createProcessingState(
        messageId,
        currentState,
        processingState,
        nextState,
        queueName
      );

      // Find and execute handler
      await this._executeMessageHandler(
        message,
        messageId,
        currentState,
        processingState,
        nextState,
        headers,
        queueName
      );
    } catch (error) {
      logger.error(`Error processing message ${messageId} from queue ${queueName}:`, error);

      // Delete processing state on error
      await this._messageTracker.deleteProcessingState(messageId, currentState);

      // Reject message
      await message.reject(false); // Don't requeue
    }
  }

  /**
   * Validate that message has all required headers
   * @param {Object} message - RabbitMQ message
   * @param {string} messageId - Message ID
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @param {string} queueName - Queue name
   * @returns {boolean} - True if valid
   * @private
   */
  _validateMessageHeaders(message, messageId, currentState, processingState, nextState, queueName) {
    if (!messageId) {
      logger.warn(`Received message without ID from queue ${queueName}, rejecting`);
      message.reject(false).catch(err => {
        logger.error(`Error rejecting message: ${err.message}`);
      });
      return false;
    }

    if (!currentState || !processingState || !nextState) {
      logger.warn(
        `Received message ${messageId} without state information from queue ${queueName}, rejecting`
      );
      message.reject(false).catch(err => {
        logger.error(`Error rejecting message: ${err.message}`);
      });
      return false;
    }

    return true;
  }

  /**
   * Check if message is already being processed
   * @param {string} messageId - Message ID
   * @param {string} currentState - Current state
   * @returns {Promise<boolean>} - True if already processing
   * @private
   */
  async _isMessageAlreadyProcessing(messageId, currentState) {
    const isProcessing = await this._messageTracker.isProcessing(messageId, currentState);
    if (isProcessing) {
      logger.warn(
        `Message ${messageId} already being processed in state ${currentState}, skipping`
      );
      return true;
    }
    return false;
  }

  /**
   * Create processing state for a message
   * @param {string} messageId - Message ID
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @param {string} queueName - Queue name
   * @returns {Promise<void>}
   * @private
   */
  async _createProcessingState(messageId, currentState, processingState, nextState, queueName) {
    await this._messageTracker.createProcessingState(messageId, currentState, {
      processingState,
      nextState,
      queueName,
      startedAt: Date.now(),
    });
  }

  /**
   * Execute the appropriate handler for a message
   * @param {Object} message - RabbitMQ message
   * @param {string} messageId - Message ID
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @param {Object} headers - Message headers
   * @param {string} queueName - Queue name
   * @returns {Promise<void>}
   * @private
   */
  async _executeMessageHandler(
    message,
    messageId,
    currentState,
    processingState,
    nextState,
    headers,
    queueName
  ) {
    const stateKey = this._getStateTransitionKey(currentState, processingState, nextState);

    if (this._stateHandlers.has(stateKey)) {
      const handler = this._stateHandlers.get(stateKey);

      // Parse message content
      let content;
      try {
        content = message.json();
      } catch (error) {
        content = message.content.toString();
      }

      // Process message
      await handler(content, {
        messageId,
        currentState,
        processingState,
        nextState,
        headers,
        queueName,
      });

      // Delete processing state from Redis
      await this._messageTracker.deleteProcessingState(messageId, currentState);

      // Acknowledge message
      await message.ack();

      logger.debug(
        `Successfully processed message ${messageId} with state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );
    } else {
      logger.warn(
        `No handler for state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );

      // Delete processing state since we're not handling it
      await this._messageTracker.deleteProcessingState(messageId, currentState);

      await message.reject(false); // Don't requeue
    }
  }

  /**
   * Get a unique key for state transition
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @returns {string} - State transition key
   * @private
   */
  _getStateTransitionKey(currentState, processingState, nextState) {
    return `${currentState}:${processingState}:${nextState}`;
  }
}
