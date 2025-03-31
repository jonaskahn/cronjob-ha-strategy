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
      prefetch: config.rabbitmq?.prefetch || 10,
      defaultQueueOptions: config.rabbitmq?.defaultQueueOptions || { durable: true },
      defaultExchangeOptions: config.rabbitmq?.defaultExchangeOptions || { durable: true },
      defaultExchangeType: config.rabbitmq?.defaultExchangeType || 'direct',
    };

    this._stateHandlers = new Map();
    this._activeConsumers = new Map();
    this._declaredQueues = new Set();
    this._declaredExchanges = new Map();

    this._coordinator.onAssignmentChanged = this._handleAssignmentChange.bind(this);
    logger.info(
      'QueueManager initialized with defaultExchangeType:',
      this._options.defaultExchangeType
    );
  }

  /**
   * Initialize the QueueManager and start processing assigned queues
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      // Get assigned queues from coordinator
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
   * Register a handler for a specific queue and state transition
   * @param {string} queueName - Queue name
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next (final) state
   * @param {Function} handler - Handler function (message, metadata) => Promise<void>
   * @param {Object} [bindingInfo] - Optional queue binding information
   * @param {string} bindingInfo.exchangeName - Exchange name
   * @param {string} bindingInfo.routingKey - Routing key
   * @param {string} [bindingInfo.exchangeType] - Exchange type (direct, topic, fanout)
   * @param {number} [priority=1] - Queue priority (1=low, 2=medium, 3=high)
   * @param {Object} [ackOptions] - Message acknowledgement options
   * @param {boolean} [ackOptions.autoAck=true] - Automatically acknowledge on success
   * @param {boolean} [ackOptions.autoReject=true] - Automatically reject on error
   * @param {boolean} [ackOptions.requeue=false] - Whether to requeue rejected messages
   */
  registerQueueStateHandler(
    queueName,
    currentState,
    processingState,
    nextState,
    handler,
    bindingInfo = null,
    priority = 1,
    ackOptions = {
      autoAck: true,
      autoReject: true,
      requeue: false,
    }
  ) {
    const stateKey = `${queueName}:${this._getStateTransitionKey(currentState, processingState, nextState)}`;

    // Store the handler and acknowledgement options
    this._stateHandlers.set(stateKey, {
      handler,
      ackOptions,
    });

    // Register binding information with coordinator if provided
    if (bindingInfo) {
      this._coordinator
        .setQueuePriority(queueName, priority, bindingInfo)
        .catch(error => logger.error(`Error registering binding for queue ${queueName}:`, error));
    } else {
      // Just set priority without binding info
      this._coordinator
        .setQueuePriority(queueName, priority)
        .catch(error => logger.error(`Error setting priority for queue ${queueName}:`, error));
    }

    logger.info(
      `Registered handler for queue ${queueName} state transition: ${currentState} -> ${processingState} -> ${nextState} (autoAck: ${ackOptions.autoAck}, autoReject: ${ackOptions.autoReject}, requeue: ${ackOptions.requeue})`
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

      // Get binding information for this queue
      const bindingInfo = await this._coordinator.getQueueBinding(queueName);

      // First ensure queue exists
      if (!this._declaredQueues.has(queueName)) {
        await this._safeAssertQueue(queueName);
        this._declaredQueues.add(queueName);
      }

      // Then create binding if binding info exists
      if (bindingInfo) {
        try {
          await this.bindQueueToExchange(
            queueName,
            bindingInfo.exchangeName,
            bindingInfo.routingKey,
            bindingInfo.exchangeType
          );
          logger.info(
            `Created binding for queue ${queueName} to exchange ${bindingInfo.exchangeName}`
          );
        } catch (bindError) {
          logger.error(`Error creating binding for queue ${queueName}:`, bindError);
          // Continue even if binding fails - we can still consume from the queue
        }
      }

      // Start consuming from the queue
      const consumerTag = await this._rabbitMQClient.consume(
        queueName,
        message => this._processMessage(message, queueName),
        { prefetch: this._options.prefetch }
      );

      // Track active consumer
      this._activeConsumers.set(queueName, consumerTag);

      // Make this log more visible for initial consumption
      logger.info('========================================');
      logger.info(`✅ STARTED CONSUMING FROM QUEUE: ${queueName}`);
      logger.info(`   Consumer Tag: ${consumerTag}`);
      logger.info(`   Prefetch: ${this._options.prefetch}`);
      logger.info('========================================');

      return consumerTag;
    } catch (error) {
      logger.error(`❌ ERROR STARTING CONSUMPTION FROM QUEUE ${queueName}:`, error);
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
      if (!this._activeConsumers.has(queueName)) {
        logger.debug(`Not consuming from queue ${queueName}, nothing to stop`);
        return true;
      }
      await this._rabbitMQClient.cancelConsumer(this._activeConsumers.get(queueName));
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
   * Bind a queue to an exchange with routing key
   * @param {string} queueName - Queue name
   * @param {string} exchangeName - Exchange name
   * @param {string} routingKey - Routing key
   * @param {string} [exchangeType] - Optional exchange type (defaults to config)
   * @returns {Promise<boolean>} - Success indicator
   */
  async bindQueueToExchange(queueName, exchangeName, routingKey, exchangeType = null) {
    try {
      // Ensure queue exists
      if (!this._declaredQueues.has(queueName)) {
        await this._safeAssertQueue(queueName);
        this._declaredQueues.add(queueName);
      }

      // Use safe exchange assertion with type adaptation
      const actualExchangeType = await this._safeEnsureExchangeExists(
        exchangeName,
        exchangeType || this._options.defaultExchangeType
      );

      // Bind the queue
      await this._rabbitMQClient.bindQueue(queueName, exchangeName, routingKey);

      logger.info(
        `Bound queue ${queueName} to exchange ${exchangeName} with routing key ${routingKey}`
      );
      return true;
    } catch (error) {
      logger.error(`Error binding queue ${queueName} to exchange ${exchangeName}:`, error);
      throw error;
    }
  }

  /**
   * Safely ensure an exchange exists, handling type mismatches
   * @param {string} exchangeName - Exchange name
   * @param {string} exchangeType - Desired exchange type
   * @returns {Promise<string>} - The actual exchange type used
   * @private
   */
  async _safeEnsureExchangeExists(exchangeName, exchangeType) {
    if (!exchangeName) return exchangeType;

    // If we already know this exchange, use the known type
    if (this._declaredExchanges.has(exchangeName)) {
      const existingType = this._declaredExchanges.get(exchangeName);
      if (existingType !== exchangeType) {
        logger.warn(
          `Using existing exchange type '${existingType}' for exchange '${exchangeName}' instead of requested '${exchangeType}'`
        );
      }
      return existingType;
    }

    try {
      // Try to create with requested type
      await this._rabbitMQClient.assertExchange(exchangeName, exchangeType);
      this._declaredExchanges.set(exchangeName, exchangeType);
      return exchangeType;
    } catch (error) {
      // Check if error is due to type mismatch
      if (
        error.message &&
        error.message.includes('PRECONDITION_FAILED') &&
        error.message.includes("inequivalent arg 'type'")
      ) {
        // Extract the actual type from error message
        const match = error.message.match(/received '(\w+)' but current is '(\w+)'/);
        if (match && match[2]) {
          const actualType = match[2];
          logger.warn(
            `Exchange '${exchangeName}' exists with type '${actualType}'. Adapting to use this type.`
          );

          // Try again with the actual type
          try {
            await this._rabbitMQClient.assertExchange(exchangeName, actualType);
            this._declaredExchanges.set(exchangeName, actualType);
            return actualType;
          } catch (retryError) {
            logger.error(`Failed to assert exchange with actual type:`, retryError);
            throw retryError;
          }
        }
      }

      logger.error(`Error ensuring exchange ${exchangeName} exists:`, error);
      throw error;
    }
  }

  /**
   * Safely assert a queue exists with retry mechanism
   * @param {string} queueName - Queue name
   * @returns {Promise<void>}
   * @private
   */
  async _safeAssertQueue(queueName) {
    try {
      await this._rabbitMQClient.assertQueue(queueName, this._options.defaultQueueOptions);
    } catch (error) {
      logger.error(`Error asserting queue ${queueName}:`, error);

      // If we get a connection error, wait and retry once
      if (
        error.message &&
        (error.message.includes('Connection closed') || error.message.includes('Channel closed'))
      ) {
        logger.info(`Retrying queue assertion for ${queueName} after connection issue`);
        await new Promise(resolve => setTimeout(resolve, 1000));
        await this._rabbitMQClient.assertQueue(queueName, this._options.defaultQueueOptions);
      } else {
        throw error;
      }
    }
  }

  /**
   * Shutdown QueueManager and clean up resources
   * @returns {Promise<void>}
   */
  async shutdown() {
    try {
      const activeQueues = this.getActiveQueues();
      for (const queueName of activeQueues) {
        await this.stopConsumingQueue(queueName);
      }
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

      if (queuesToStop.length > 0 || queuesToStart.length > 0) {
        logger.info('========================================');
        logger.info('QUEUE ASSIGNMENT CHANGE DETECTED');
        logger.info('========================================');
      }

      // Stop consuming from removed queues - with individual error handling
      if (queuesToStop.length > 0) {
        logger.info(`STOPPING CONSUMPTION OF ${queuesToStop.length} QUEUES:`);
        queuesToStop.forEach(q => logger.info(`   - ${q}`));

        for (const queueName of queuesToStop) {
          try {
            await this.stopConsumingQueue(queueName);
            logger.info(`✅ Stopped consuming from queue: ${queueName}`);
          } catch (stopError) {
            logger.error(`❌ Failed to stop queue ${queueName}:`, stopError);
            // Continue trying to stop other queues despite this error
          }
        }
      }

      // Start consuming from new queues - with individual error handling
      if (queuesToStart.length > 0) {
        logger.info(`STARTING CONSUMPTION OF ${queuesToStart.length} QUEUES:`);
        queuesToStart.forEach(q => logger.info(`   - ${q}`));

        for (const queueName of queuesToStart) {
          try {
            await this.startConsumingQueue(queueName);
            logger.info(`✅ Started consuming from queue: ${queueName}`);
          } catch (startError) {
            logger.error(`❌ Failed to start queue ${queueName}:`, startError);
            // Continue trying to start other queues despite this error
          }
        }
      }

      if (queuesToStop.length > 0 || queuesToStart.length > 0) {
        logger.info('========================================');
        logger.info(
          `QUEUE ASSIGNMENTS UPDATED: Stopped ${queuesToStop.length}, Started ${queuesToStart.length}`
        );
        logger.info(`CURRENT ACTIVE QUEUES: ${this.getActiveQueues().join(', ')}`);
        logger.info('========================================');
      }
    } catch (error) {
      logger.error('❌ ERROR HANDLING ASSIGNMENT CHANGE:', error);
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
      // Find the queue-specific state handler
      const stateKey = `${queueName}:${this._getStateTransitionKey(currentState, processingState, nextState)}`;

      if (this._stateHandlers.has(stateKey)) {
        const { handler, ackOptions = { autoAck: true, autoReject: true, requeue: false } } =
          this._stateHandlers.get(stateKey);

        // Parse message content
        let content;
        try {
          content = message.json();
        } catch (error) {
          content = message.content.toString();
        }

        // Process message with handler
        const success = await handler(content, {
          messageId,
          queueName,
          currentState,
          processingState,
          nextState,
          headers,
          message: {
            ack: () => message.ack(),
            nack: (requeue = false) => message.nack(requeue),
            reject: (requeue = false) => message.reject(requeue),
          },
        });
        if (success) {
          if (ackOptions.autoAck) {
            await message.ack();
            logger.debug(`Message ${messageId} auto-acknowledged after successful processing`);
          } else {
            logger.debug(
              `Message ${messageId} processed successfully, but autoAck disabled - manual acknowledgement required`
            );
          }
        } else if (success === false) {
          if (ackOptions.autoReject) {
            await message.reject(ackOptions.requeue);
            logger.debug(
              `Message ${messageId} auto-rejected (requeue: ${ackOptions.requeue}) after handler returned false`
            );
          } else {
            logger.debug(
              `Message ${messageId} processing failed, but autoReject disabled - manual rejection required`
            );
          }
        }
      } else {
        logger.warn(
          `No handler for queue ${queueName} state transition: ${currentState} -> ${processingState} -> ${nextState}`
        );

        await message.reject(false); // Don't requeue
      }
    } catch (error) {
      logger.error(`Error processing message ${messageId} from queue ${queueName}:`, error);

      // Find ackOptions to determine error behavior
      const stateKey = `${queueName}:${this._getStateTransitionKey(currentState, processingState, nextState)}`;
      const { ackOptions = { autoAck: true, autoReject: true, requeue: false } } =
        this._stateHandlers.get(stateKey) || {};

      try {
        // Clean up processing state if an error occurs
        if (this._messageTracker) {
          await this._messageTracker.deleteProcessingState(messageId, currentState);
        }
      } catch (redisError) {
        logger.warn(`Error cleaning up processing state: ${redisError.message}`);
      }

      // Auto-reject on error if enabled
      if (ackOptions.autoReject) {
        await message.reject(ackOptions.requeue);
        logger.debug(
          `Message ${messageId} auto-rejected (requeue: ${ackOptions.requeue}) after processing error`
        );
      } else {
        logger.debug(
          `Message ${messageId} processing threw error, but autoReject disabled - manual handling required`
        );
      }
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
