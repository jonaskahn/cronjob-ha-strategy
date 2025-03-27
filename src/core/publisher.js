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
   * @param {AppConfig} config - System configuration
   */
  constructor(rabbitMQClient, messageTracker, config) {
    this._rabbitMQClient = rabbitMQClient;
    this._messageTracker = messageTracker;
    this._options = {
      defaultExchangeType: config.publisher.defaultExchangeType,
      confirmPublish: config.publisher.confirmPublish,
      defaultHeaders: config.publisher.defaultHeaders,
    };

    this._exchanges = new Map();
    this._publishCount = 0;
    this._errorCount = 0;

    logger.info('Publisher initialized');
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
    } = options;

    this._validatePublishOptions(messageId, currentState, processingState, nextState);

    try {
      if (await this._isDuplicateMessage(messageId, currentState)) {
        return false;
      }
      await this._ensureExchangeExists(exchangeName, exchangeType);
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
    } = options;

    // Validate required fields
    this._validatePublishOptions(messageId, currentState, processingState, nextState);

    try {
      // Check for duplicate processing
      if (await this._isDuplicateMessage(messageId, currentState)) {
        return false;
      }

      // Ensure queue exists
      await this._rabbitMQClient.assertQueue(queueName);

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
   * Publish a batch of messages with the same state transition
   * @param {Array<Object>} messages - Array of message objects
   * @param {string} exchangeName - Exchange name
   * @param {string} routingKey - Routing key
   * @param {string} currentState - Current state for all messages
   * @param {string} processingState - Processing state for all messages
   * @param {string} nextState - Next state for all messages
   * @param {Object} [options] - Additional options
   * @returns {Promise<Array<Object>>} - Array of results
   */
  async publishBatch(
    messages,
    exchangeName,
    routingKey,
    currentState,
    processingState,
    nextState,
    options = {}
  ) {
    if (!Array.isArray(messages) || messages.length === 0) {
      throw new Error('Messages must be a non-empty array');
    }

    // Check for processing statuses in batch for efficiency
    const messagesToCheck = messages.map(msg => ({
      messageId: msg.id,
      processingState: currentState,
    }));

    const processingStatuses = await this._getProcessingStatusesForBatch(messagesToCheck);
    return await this._processMessageBatch(
      messages,
      processingStatuses,
      exchangeName,
      routingKey,
      currentState,
      processingState,
      nextState,
      options
    );
  }

  /**
   * Get processing statuses for a batch of messages
   * @param {Array<Object>} messagesToCheck - Array of {messageId, processingState} objects
   * @returns {Promise<Object>} - Map of messageId to processing status
   * @private
   */
  async _getProcessingStatusesForBatch(messagesToCheck) {
    const processingStatuses = {};

    if (this._messageTracker) {
      const batchStatus = await this._messageTracker.batchIsProcessing(messagesToCheck);
      Object.assign(processingStatuses, batchStatus);
    }

    return processingStatuses;
  }

  /**
   * Process a batch of messages
   * @param {Array<Object>} messages - Array of message objects
   * @param {Object} processingStatuses - Map of messageId to processing status
   * @param {string} exchangeName - Exchange name
   * @param {string} routingKey - Routing key
   * @param {string} currentState - Current state for all messages
   * @param {string} processingState - Processing state for all messages
   * @param {string} nextState - Next state for all messages
   * @param {Object} options - Additional options
   * @returns {Promise<Array<Object>>} - Array of results
   * @private
   */
  async _processMessageBatch(
    messages,
    processingStatuses,
    exchangeName,
    routingKey,
    currentState,
    processingState,
    nextState,
    options
  ) {
    const results = [];
    const exchangeType = options.exchangeType || this._options.defaultExchangeType;

    // Ensure exchange exists
    if (exchangeName) {
      await this._ensureExchangeExists(exchangeName, exchangeType);
    }

    // Process each message
    for (const msg of messages) {
      try {
        if (!msg.id) {
          throw new Error('Each message must have an id property');
        }

        // Skip if already processing
        if (processingStatuses[msg.id]) {
          logger.warn(
            `Message ${msg.id} is already being processed in state ${currentState}, skipping`
          );

          results.push({
            id: msg.id,
            success: false,
            skipped: true,
            error: 'Message already being processed',
          });

          continue;
        }

        const publishResult = await this.publish({
          exchangeName,
          exchangeType,
          routingKey,
          message: msg.content || msg,
          messageId: msg.id,
          currentState,
          processingState,
          nextState,
          headers: msg.headers || {},
          publishOptions: msg.options || {},
        });

        results.push({
          id: msg.id,
          success: true,
          result: publishResult,
        });
      } catch (error) {
        results.push({
          id: msg.id,
          success: false,
          error: error.message,
        });
      }
    }

    const successCount = results.filter(r => r.success).length;
    logger.info(`Published batch: ${successCount}/${messages.length} successful`);

    return results;
  }

  /**
   * Get publishing statistics
   * @returns {Object} - Statistics object
   */
  getStats() {
    return {
      publishCount: this._publishCount,
      errorCount: this._errorCount,
      declaredExchanges: Array.from(this._exchanges.keys()),
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
   * Ensure an exchange exists
   * @param {string} exchangeName - Exchange name
   * @param {string} exchangeType - Exchange type
   * @returns {Promise<void>}
   * @private
   */
  async _ensureExchangeExists(exchangeName, exchangeType) {
    if (exchangeName && !this._exchanges.has(exchangeName)) {
      await this._rabbitMQClient.assertExchange(exchangeName, exchangeType);
      this._exchanges.set(exchangeName, exchangeType);
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

  /**
   * Implement retry for publishing with exponential backoff
   * @param {Function} publishFunc - Publishing function to retry
   * @param {number} [maxRetries=3] - Maximum retry attempts
   * @param {number} [initialDelayMs=100] - Initial delay in milliseconds
   * @returns {Promise<boolean>} - Success indicator
   */
  async publishWithRetry(publishFunc, maxRetries = 3, initialDelayMs = 100) {
    let retryCount = 0;
    let lastError = null;

    while (retryCount <= maxRetries) {
      try {
        return await publishFunc();
      } catch (error) {
        lastError = error;
        retryCount++;

        if (retryCount > maxRetries) {
          logger.error(`Publishing failed after ${maxRetries} retries:`, error);
          break;
        }

        const delay = initialDelayMs * Math.pow(2, retryCount - 1);
        logger.warn(
          `Publish attempt ${retryCount} failed, retrying in ${delay}ms: ${error.message}`
        );

        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }

    // If we got here, all retries failed
    throw lastError || new Error(`Publishing failed after ${maxRetries} retries`);
  }
}
