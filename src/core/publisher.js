import getLogger from '../utils/logger.js';

const logger = getLogger('core/Publisher');

/**
 * Publisher class for sending messages with state transition information
 * to RabbitMQ exchanges and queues.
 */
export default class Publisher {
  /**
   * Create a new Publisher instance
   * @param {RabbitMQClient} rabbitMQClient - RabbitMQ client instance
   * @param {MessageTracker} messageTracker - MessageTracker instance to check for duplicates
   * @param {Coordinator} coordinator - Coordinator instance for queue priorities
   * @param {AppConfig} config - System configuration
   */
  constructor(rabbitMQClient, messageTracker, coordinator, config) {
    this._rabbitMQClient = rabbitMQClient;
    this._messageTracker = messageTracker;
    this._coordinator = coordinator;
    this._options = {
      defaultExchangeType: config.publisher?.defaultExchangeType || 'direct',
      confirmPublish: config.publisher?.confirmPublish,
      defaultHeaders: config.publisher?.defaultHeaders || {},
    };

    this._exchanges = new Map();
    this._publishCount = 0;
    this._errorCount = 0;

    logger.info(
      'Publisher initialized with defaultExchangeType:',
      this._options.defaultExchangeType
    );
  }

  /**
   * Publish a message with state transition information
   * @param {Object} options - Publish options
   * @param {string} options.exchangeName - Exchange name
   * @param {string} [options.exchangeType] - Exchange type (direct, topic, fanout)
   * @param {string} options.routingKey - Routing key
   * @param {Object|string|Buffer} options.message - Message content
   * @param {string|number} options.messageId - Message ID for deduplication
   * @param {string} options.currentState - Current state
   * @param {string} options.processingState - Processing state
   * @param {string} options.nextState - Next state after processing
   * @param {Object} [options.headers] - Additional message headers
   * @param {Object} [options.publishOptions] - Additional publish options
   * @param {number} [options.priority] - Queue priority (1=low, 2=medium, 3=high)
   * @returns {Promise<boolean>} - Success indicator
   */
  async publish(options) {
    const {
      exchangeName,
      exchangeType = this._options.defaultExchangeType,
      routingKey,
      message,
      messageId,
      currentState,
      processingState,
      nextState,
      headers = {},
      publishOptions = {},
      priority,
    } = options;

    this._validatePublishOptions(messageId, currentState, processingState, nextState);

    try {
      if (await this._isDuplicateMessage(messageId, currentState)) {
        return false;
      }

      // If a priority was specified, store the queue binding information with priority
      if (priority && this._coordinator && routingKey && exchangeName) {
        const bindingInfo = {
          exchangeName,
          routingKey,
          exchangeType,
        };

        // Note: This assumes the routingKey is the queue name, which is common but not guaranteed
        // In a real system, you might need more mapping info
        await this._coordinator.setQueuePriority(routingKey, priority, bindingInfo);
      }

      // Use safe exchange initialization to handle type mismatches
      await this._safeEnsureExchangeExists(exchangeName, exchangeType);
      const messageContent = this._prepareMessageContent(message, {
        messageId,
        currentState,
        processingState,
        nextState,
      });

      const messageHeaders = this._prepareMessageHeaders({
        ...headers,
        messageId,
        currentState,
        processingState,
        nextState,
      });

      const finalPublishOptions = this._prepareFinalPublishOptions(
        publishOptions,
        messageHeaders,
        messageId,
        message
      );

      // Publish the message
      const result = await this._rabbitMQClient.publish(
        exchangeName,
        routingKey,
        messageContent,
        finalPublishOptions
      );

      this._publishCount++;
      logger.debug(
        `Published message ${messageId} to ${exchangeName}/${routingKey} with state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );

      return result;
    } catch (error) {
      this._errorCount++;
      logger.error(`Error publishing message ${messageId}:`, error);
      throw error;
    }
  }

  /**
   * Publish a message directly to a queue
   * @param {Object} options - Publish options
   * @param {string} options.queueName - Queue name
   * @param {Object|string|Buffer} options.message - Message content
   * @param {string|number} options.messageId - Message ID for deduplication
   * @param {string} options.currentState - Current state
   * @param {string} options.processingState - Processing state
   * @param {string} options.nextState - Next state after processing
   * @param {Object} [options.headers] - Additional message headers
   * @param {Object} [options.publishOptions] - Additional publish options
   * @param {number} [options.priority] - Queue priority (1=low, 2=medium, 3=high)
   * @returns {Promise<boolean>} - Success indicator
   */
  async publishToQueue(options) {
    const {
      queueName,
      message,
      messageId,
      currentState,
      processingState,
      nextState,
      headers = {},
      publishOptions = {},
      priority,
    } = options;

    // Validate required fields
    this._validatePublishOptions(messageId, currentState, processingState, nextState);

    try {
      // Check for duplicate processing
      if (await this._isDuplicateMessage(messageId, currentState)) {
        return false;
      }

      // If a priority was specified, store it
      if (priority && this._coordinator) {
        await this._coordinator.setQueuePriority(queueName, priority);
      }

      // Ensure queue exists with retry mechanism
      await this._safeAssertQueue(queueName);

      // Prepare message content and options
      const messageContent = this._prepareMessageContent(message, {
        messageId,
        currentState,
        processingState,
        nextState,
      });

      const messageHeaders = this._prepareMessageHeaders({
        ...headers,
        messageId,
        currentState,
        processingState,
        nextState,
      });

      const finalPublishOptions = this._prepareFinalPublishOptions(
        publishOptions,
        messageHeaders,
        messageId,
        message
      );

      // Send directly to queue
      const result = await this._rabbitMQClient.sendToQueue(
        queueName,
        messageContent,
        finalPublishOptions
      );

      this._publishCount++;
      logger.debug(
        `Published message ${messageId} to queue ${queueName} with state transition: ${currentState} -> ${processingState} -> ${nextState}`
      );

      return result;
    } catch (error) {
      this._errorCount++;
      logger.error(`Error publishing message ${messageId} to queue ${queueName}:`, error);
      throw error;
    }
  }

  /**
   * Get publishing statistics
   * @returns {Object} - Statistics object
   */
  getStats() {
    return {
      publishCount: this._publishCount,
      errorCount: this._errorCount,
      declaredExchanges: Array.from(this._exchanges.entries()).map(([name, type]) => ({
        name,
        type,
      })),
    };
  }

  /**
   * Reset publishing statistics
   */
  resetStats() {
    this._publishCount = 0;
    this._errorCount = 0;
  }

  /**
   * Validate publish options for required fields
   * @param {string} messageId - Message ID
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @throws {Error} - If required fields are missing
   * @private
   */
  _validatePublishOptions(messageId, currentState, processingState, nextState) {
    if (!messageId) {
      throw new Error('Message ID is required for publishing');
    }

    if (!currentState || !processingState || !nextState) {
      throw new Error('State information (currentState, processingState, nextState) is required');
    }
  }

  /**
   * Check if a message is a duplicate (already being processed)
   * @param {string} messageId - Message ID
   * @param {string} currentState - Current state
   * @returns {Promise<boolean>} - True if duplicate
   * @private
   */
  async _isDuplicateMessage(messageId, currentState) {
    if (this._messageTracker) {
      const isProcessing = await this._messageTracker.isProcessing(messageId, currentState);
      if (isProcessing) {
        logger.warn(
          `Message ${messageId} is already being processed in state ${currentState}, skipping publish`
        );
        return true;
      }
    }
    return false;
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
    if (this._exchanges.has(exchangeName)) {
      const existingType = this._exchanges.get(exchangeName);
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
      this._exchanges.set(exchangeName, exchangeType);
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
            this._exchanges.set(exchangeName, actualType);
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
      await this._rabbitMQClient.assertQueue(queueName);
    } catch (error) {
      logger.error(`Error asserting queue ${queueName}:`, error);

      // If we get a connection error, wait and retry once
      if (
        error.message &&
        (error.message.includes('Connection closed') || error.message.includes('Channel closed'))
      ) {
        logger.info(`Retrying queue assertion for ${queueName} after connection issue`);
        await new Promise(resolve => setTimeout(resolve, 1000));
        await this._rabbitMQClient.assertQueue(queueName);
      } else {
        throw error;
      }
    }
  }

  /**
   * Prepare message content with metadata
   * @param {Object|string|Buffer} message - Message content
   * @param {Object} metadata - Message metadata
   * @returns {Object|string|Buffer} - Prepared message
   * @private
   */
  _prepareMessageContent(message, metadata) {
    // If message is already a buffer or string, return as is
    if (Buffer.isBuffer(message) || typeof message === 'string') {
      return message;
    }

    // If message is an object, add metadata
    if (typeof message === 'object') {
      return {
        ...message,
        _meta: {
          ...metadata,
          timestamp: Date.now(),
        },
      };
    }

    // Default case
    return message;
  }

  /**
   * Prepare message headers
   * @param {Object} headers - Headers object
   * @returns {Object} - Prepared headers
   * @private
   */
  _prepareMessageHeaders(headers) {
    const { messageId, currentState, processingState, nextState } = headers;

    return {
      ...this._options.defaultHeaders,
      ...headers,
      'x-message-id': String(messageId),
      'x-current-state': currentState,
      'x-processing-state': processingState,
      'x-next-state': nextState,
      'x-published-at': Date.now(),
    };
  }

  /**
   * Prepare final publish options
   * @param {Object} publishOptions - Publish options
   * @param {Object} messageHeaders - Message headers
   * @param {string} messageId - Message ID
   * @param {Object|string|Buffer} message - Message content
   * @returns {Object} - Final publish options
   * @private
   */
  _prepareFinalPublishOptions(publishOptions, messageHeaders, messageId, message) {
    return {
      ...publishOptions,
      headers: messageHeaders,
      messageId: String(messageId),
      persistent: true,
      timestamp: Date.now(),
      contentType: this._getContentType(message),
    };
  }

  /**
   * Determine content type based on message
   * @param {Object|string|Buffer} message - Message content
   * @returns {string} - Content type
   * @private
   */
  _getContentType(message) {
    if (Buffer.isBuffer(message)) {
      return 'application/octet-stream';
    }

    if (typeof message === 'string') {
      return 'text/plain';
    }

    if (typeof message === 'object') {
      return 'application/json';
    }

    return 'application/octet-stream';
  }
}
