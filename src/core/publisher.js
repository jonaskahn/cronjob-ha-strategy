import getLogger from '../utils/logger.js';
import rabbitMQClient from '../client/rabbitMQClient.js';

const logger = getLogger('core/Publisher');

/**
 * Publisher class for sending messages with state transition information
 * to RabbitMQ exchanges and queues.
 */
class Publisher {
  /**
   * Create a new Publisher instance
   * @param {Object} rabbitMQClient - RabbitMQ client instance
   * @param {Object} options - Configuration options
   */
  constructor(rabbitMQClient, options = {}) {
    if (Publisher.instance) {
      return Publisher.instance;
    }

    this._rabbitMQClient = rabbitMQClient;
    this._options = {
      defaultExchangeType: 'direct',
      confirmPublish: true,
      defaultHeaders: {},
      ...options,
    };

    this._exchanges = new Map();
    this._publishCount = 0;
    this._errorCount = 0;

    Publisher.instance = this;

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

    if (!messageId) {
      throw new Error('Message ID is required for publishing');
    }

    if (!currentState || !processingState || !nextState) {
      throw new Error('State information (currentState, processingState, nextState) is required');
    }

    try {
      // Ensure exchange exists
      if (exchangeName && !this._exchanges.has(exchangeName)) {
        await this._rabbitMQClient.assertExchange(exchangeName, exchangeType);
        this._exchanges.set(exchangeName, exchangeType);
      }

      // Prepare message content
      const messageContent = this.#prepareMessageContent(message, {
        messageId,
        currentState,
        processingState,
        nextState,
      });

      // Prepare message headers
      const messageHeaders = {
        ...this._options.defaultHeaders,
        ...headers,
        'x-message-id': String(messageId),
        'x-current-state': currentState,
        'x-processing-state': processingState,
        'x-next-state': nextState,
        'x-published-at': Date.now(),
      };

      // Prepare publish options
      const finalPublishOptions = {
        ...publishOptions,
        headers: messageHeaders,
        messageId: String(messageId),
        persistent: true,
        timestamp: Date.now(),
        contentType: this.#getContentType(message),
      };

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

    if (!messageId) {
      throw new Error('Message ID is required for publishing');
    }

    if (!currentState || !processingState || !nextState) {
      throw new Error('State information (currentState, processingState, nextState) is required');
    }

    try {
      // Ensure queue exists
      await this._rabbitMQClient.assertQueue(queueName);

      // Prepare message content
      const messageContent = this.#prepareMessageContent(message, {
        messageId,
        currentState,
        processingState,
        nextState,
      });

      // Prepare message headers
      const messageHeaders = {
        ...this._options.defaultHeaders,
        ...headers,
        'x-message-id': String(messageId),
        'x-current-state': currentState,
        'x-processing-state': processingState,
        'x-next-state': nextState,
        'x-published-at': Date.now(),
      };

      // Prepare publish options
      const finalPublishOptions = {
        ...publishOptions,
        headers: messageHeaders,
        messageId: String(messageId),
        persistent: true,
        timestamp: Date.now(),
        contentType: this.#getContentType(message),
      };

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

    const results = [];
    const exchangeType = options.exchangeType || this._options.defaultExchangeType;

    // Ensure exchange exists
    if (exchangeName && !this._exchanges.has(exchangeName)) {
      await this._rabbitMQClient.assertExchange(exchangeName, exchangeType);
      this._exchanges.set(exchangeName, exchangeType);
    }

    // Process each message
    for (const msg of messages) {
      try {
        if (!msg.id) {
          throw new Error('Each message must have an id property');
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
   * Prepare message content with metadata
   * @param {Object|string|Buffer} message - Message content
   * @param {Object} metadata - Message metadata
   * @returns {Object|string|Buffer} - Prepared message
   * @private
   */
  #prepareMessageContent(message, metadata) {
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
   * Determine content type based on message
   * @param {Object|string|Buffer} message - Message content
   * @returns {string} - Content type
   * @private
   */
  #getContentType(message) {
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

// Create and export singleton instance
const publisher = new Publisher(rabbitMQClient);
export { Publisher };
export default publisher;
