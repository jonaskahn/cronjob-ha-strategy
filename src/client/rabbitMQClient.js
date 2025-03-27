import getLogger from '../utils/logger.js';
import amqp from 'amqplib';

const logger = getLogger('client/RabbitMQClient');

/**
 * RabbitMQ client with support for multiple exchange types
 * and message deduplication
 */
export default class RabbitMQClient {
  /**
   * Create a new RabbitMQ client
   * @param {AppConfig} config - System configuration
   */
  constructor(config) {
    this._config = config.rabbitmq;
    this._client = null;
    this._reconnecting = false;
    this._connected = false;
    this._channels = new Map();
    this._consumers = new Map();
    this._publishChannel = null;
    this._exchangeTypes = new Set(['direct', 'topic', 'fanout', 'headers']);
    this._declaredExchanges = new Set();
    this._boundQueues = new Map();

    logger.info('RabbitMQ client initialized');
  }

  /**
   * Establish connection to RabbitMQ
   * @returns {Promise<void>}
   */
  async connect() {
    if (this._connected || this._reconnecting) {
      logger.debug('RabbitMQ connection already established or in progress');
      return;
    }

    try {
      logger.debug('Starting RabbitMQ connection');
      this._client = await amqp.connect(`amqp://${this._config.connectionUrl}`, {
        heartbeat: this._config.heartbeat,
      });

      this._client.on('error', async err => {
        logger.error('RabbitMQ connection error:', err);
        this._connected = false;
        await this._reconnect();
      });

      this._client.on('close', () => {
        if (this._connected) {
          logger.warn('RabbitMQ connection closed unexpectedly');
          this._connected = false;
        }
      });

      this._connected = true;
      logger.info('RabbitMQ connection established');
    } catch (error) {
      this._connected = false;
      logger.error('Failed to connect to RabbitMQ:', error);
      throw error;
    }
  }

  /**
   * Reconnect to RabbitMQ after connection failure
   * @returns {Promise<void>}
   * @private
   */
  async _reconnect() {
    if (this._reconnecting) return;

    this._reconnecting = true;
    logger.info('Initiating RabbitMQ reconnection sequence');

    try {
      await this._cleanupExistingConnection();
      await this._reconnectWithBackoff();
      await this._redeclareExchangesAndBindings();
      await this._restoreConsumers();
      logger.info('RabbitMQ reconnection completed successfully');
      this._connected = true;
    } catch (error) {
      logger.error('RabbitMQ reconnection failed:', error);
      this._connected = false;
    } finally {
      this._reconnecting = false;
    }
  }

  /**
   * Clean up the existing connection and channels
   * @returns {Promise<void>}
   * @private
   */
  async _cleanupExistingConnection() {
    try {
      for (const [queueName, channel] of this._channels.entries()) {
        try {
          await channel.close();
        } catch (error) {
          logger.warn(`Error closing channel for ${queueName}:`, error);
        }
      }
      this._channels.clear();

      if (this._publishChannel) {
        try {
          await this._publishChannel.close();
        } catch (error) {
          logger.warn('Error closing publish channel:', error);
        }
        this._publishChannel = null;
      }

      if (this._client) {
        await this._client.close();
        this._client = null;
      }
    } catch (error) {
      logger.warn('Error during connection cleanup:', error);
    }
  }

  /**
   * Reconnect with exponential backoff
   * @param {number} [attempt=1] - Current attempt number
   * @returns {Promise<void>}
   * @private
   */
  async _reconnectWithBackoff(attempt = 1) {
    const MAX_ATTEMPTS = this._config.maxReconnectAttempts || 10;
    const MAX_DELAY_MS = this._config.maxReconnectDelay || 30_000;

    try {
      this._client = await amqp.connect(`amqp://${this._config.connectionUrl}`, {
        heartbeat: this._config.heartbeat,
      });

      this._client.on('error', async err => {
        logger.error('RabbitMQ connection error:', err);
        this._connected = false;
        await this._reconnect();
      });
    } catch (error) {
      if (attempt >= MAX_ATTEMPTS) {
        logger.error(`Reconnection failed after ${MAX_ATTEMPTS} attempts`);
        throw error;
      }

      const delayMs = Math.min(
        this._config.reconnectDelay * Math.pow(2, attempt - 1),
        MAX_DELAY_MS
      );
      logger.info(`Reconnection attempt ${attempt} failed, retrying in ${delayMs}ms`);

      await new Promise(resolve => setTimeout(resolve, delayMs));
      await this._reconnectWithBackoff(attempt + 1);
    }
  }

  /**
   * Redeclare exchanges and queue bindings after reconnection
   * @returns {Promise<void>}
   * @private
   */
  async _redeclareExchangesAndBindings() {
    try {
      // Redeclare exchanges
      for (const exchangeName of this._declaredExchanges) {
        // We don't store exchange types, so we use the default
        await this.assertExchange(exchangeName, this._config.defaultExchangeType);
        logger.info(`Redeclared exchange: ${exchangeName}`);
      }

      // Redeclare bindings
      for (const [queueName, bindings] of this._boundQueues.entries()) {
        await this.assertQueue(queueName);
        for (const binding of bindings) {
          await this.bindQueue(queueName, binding.exchange, binding.pattern);
          logger.info(
            `Redeclared binding: ${queueName} -> ${binding.exchange} (${binding.pattern})`
          );
        }
      }
    } catch (error) {
      logger.error('Error redeclaring exchanges and bindings:', error);
    }
  }

  /**
   * Restore consumer handlers after reconnection
   * @returns {Promise<void>}
   * @private
   */
  async _restoreConsumers() {
    for (const [queue, consumerInfo] of this._consumers.entries()) {
      try {
        // Use the stored handler and options
        const { handler, options } = consumerInfo;
        await this.consume(queue, handler, options);
        logger.info(`Restored consumer for queue: ${queue}`);
      } catch (error) {
        logger.error(`Failed to restore consumer for queue ${queue}:`, error);
      }
    }
  }

  /**
   * List all available queues
   * @returns {Promise<Array>} - Array of queue objects
   */
  async listQueues() {
    await this._ensureConnected();

    try {
      return await this._fetchCurrentQueues();
    } catch (error) {
      logger.error('Error listing queues:', error);
      throw error;
    }
  }

  /**
   * Fetch current queues from RabbitMQ management API
   * @returns {Promise<Array>} - Array of queue objects
   * @private
   */
  async _fetchCurrentQueues() {
    const connectionUrl = this._config.managerHost;
    const [username, password] = connectionUrl.split('@')[0].split(':');
    const host = connectionUrl.split('@')[1].split(':')[0];
    const port = connectionUrl.split('@')[1].split(':')[1];

    const response = await fetch(`http://${host}:${port}/api/queues`, {
      headers: {
        Authorization: `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`,
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to list queues: ${response.statusText}`);
    }

    const queues = await response.json();
    return queues.map(queue => ({
      name: queue.name,
      vhost: queue.vhost,
      consumers: queue.consumers,
    }));
  }

  /**
   * List all available exchanges
   * @returns {Promise<Array>} - Array of exchange objects
   */
  async listExchanges() {
    await this._ensureConnected();

    try {
      const connectionUrl = this._config.managerHost;
      const [username, password] = connectionUrl.split('@')[0].split(':');
      const host = connectionUrl.split('@')[1].split(':')[0];
      const port = connectionUrl.split('@')[1].split(':')[1];

      const response = await fetch(`http://${host}:${port}/api/exchanges`, {
        headers: {
          Authorization: `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`,
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to list exchanges: ${response.statusText}`);
      }

      const exchanges = await response.json();
      return exchanges.map(exchange => ({
        name: exchange.name,
        type: exchange.type,
        vhost: exchange.vhost,
      }));
    } catch (error) {
      logger.error('Error listing exchanges:', error);
      throw error;
    }
  }

  /**
   * Ensure client is connected before performing operations
   * @returns {Promise<void>}
   * @private
   */
  async _ensureConnected() {
    if (!this._connected) {
      await this.connect();
    }
  }

  /**
   * Start consuming messages from a queue
   * @param {string} queue - Queue name
   * @param {Function} handler - Message handler function (message, queue) => Promise<void>
   * @param {Object} options - Consumer options
   * @returns {Promise<string>} - Consumer tag
   */
  async consume(queue, handler, options = {}) {
    await this._ensureConnected();

    const consumerOptions = {
      prefetch: this._config.prefetch,
      ...options,
    };

    try {
      await this.assertQueue(queue);
      const channel = await this._getChannelForQueue(queue);
      await channel.prefetch(consumerOptions.prefetch);

      const { consumerTag } = await channel.consume(queue, message =>
        this._handleIncomingMessage(message, channel, handler, queue)
      );

      this._consumers.set(queue, {
        handler,
        options: consumerOptions,
        consumerTag,
      });

      logger.debug(`Started consuming from queue ${queue} with tag ${consumerTag}`);
      return consumerTag;
    } catch (error) {
      logger.error(`Error consuming from queue ${queue}:`, error);
      throw error;
    }
  }

  /**
   * Get or create a channel for a specific queue
   * @param {string} queue - Queue name
   * @returns {Promise<Object>} - AMQP channel
   * @private
   */
  async _getChannelForQueue(queue) {
    if (this._channels.has(queue)) {
      return this._channels.get(queue);
    }
    const channel = await this._createChannelForQueue(queue);
    return channel;
  }

  /**
   * Create a new channel for a queue
   * @param {string} queue - Queue name
   * @returns {Promise<Object>} - AMQP channel
   * @private
   */
  async _createChannelForQueue(queue) {
    const channel = await this._client.createChannel();
    this._channels.set(queue, channel);

    channel.on('error', error => {
      logger.error(`Channel error for queue ${queue}:`, error);
      this._channels.delete(queue);
    });

    channel.on('close', () => {
      logger.info(`Channel closed for queue ${queue}`);
      this._channels.delete(queue);
    });

    return channel;
  }

  /**
   * Handle an incoming message from RabbitMQ
   * @param {Object} message - RabbitMQ message
   * @param {Object} channel - AMQP channel
   * @param {Function} handler - Message handler function
   * @param {string} queue - Queue name
   * @returns {Promise<void>}
   * @private
   */
  async _handleIncomingMessage(message, channel, handler, queue) {
    if (!message) return; // Consumer cancelled

    try {
      const enhancedMessage = this._enhanceMessageWithMethods(message, channel);

      // Extract deduplication ID if present
      const deduplicationId = this._extractDeduplicationId(message);
      if (deduplicationId) {
        logger.debug(`Processing message with deduplication ID: ${deduplicationId}`);
        enhancedMessage.deduplicationId = deduplicationId;
      }

      await handler(enhancedMessage, queue);
    } catch (error) {
      logger.error(`Error processing message from ${queue}:`, error);
      await channel.reject(message, false);
    }
  }

  /**
   * Extract deduplication ID from message properties
   * @param {Object} message - RabbitMQ message
   * @returns {string|null} - Deduplication ID or null
   * @private
   */
  _extractDeduplicationId(message) {
    // Try to extract deduplication ID from headers or messageId
    if (message.properties.headers && message.properties.headers['x-deduplication-id']) {
      return message.properties.headers['x-deduplication-id'];
    }

    if (message.properties.messageId) {
      return message.properties.messageId;
    }

    return null;
  }

  /**
   * Enhance a RabbitMQ message with convenience methods
   * @param {Object} message - RabbitMQ message
   * @param {Object} channel - AMQP channel
   * @returns {Object} - Enhanced message
   * @private
   */
  _enhanceMessageWithMethods(message, channel) {
    return {
      ...message,
      content: message.content,
      json: () => {
        try {
          return JSON.parse(message.content.toString());
        } catch (error) {
          logger.error('Failed to parse message content as JSON:', error);
          return null;
        }
      },
      text: () => {
        return message.content.toString();
      },
      ack: async () => {
        await channel.ack(message);
      },
      reject: async (requeue = false) => {
        await channel.reject(message, requeue);
      },
      nack: async (requeue = false, allUpTo = false) => {
        await channel.nack(message, allUpTo, requeue);
      },
      properties: message.properties,
    };
  }

  /**
   * Get or create a dedicated publish channel
   * @returns {Promise<Object>} - AMQP channel
   * @private
   */
  async _getPublishChannel() {
    if (!this._publishChannel) {
      this._publishChannel = await this._client.createChannel();

      this._publishChannel.on('error', async error => {
        logger.error('Publish channel error:', error);
        this._publishChannel = null;
      });

      this._publishChannel.on('close', () => {
        logger.info('Publish channel closed');
        this._publishChannel = null;
      });
    }
    return this._publishChannel;
  }

  /**
   * Publish a message to an exchange with optional deduplication ID
   * @param {string} exchange - Exchange name
   * @param {string} routingKey - Routing key
   * @param {Buffer|string|Object} content - Message content
   * @param {Object} options - Publish options
   * @param {string} [options.deduplicationId] - Optional deduplication ID
   * @returns {Promise<boolean>} - Success indicator
   */
  async publish(exchange, routingKey, content, options = {}) {
    await this._ensureConnected();

    try {
      // Ensure exchange exists if not default
      if (exchange !== '') {
        const type = options.exchangeType || this._config.defaultExchangeType;
        await this.assertExchange(exchange, type);
      }

      const channel = await this._getPublishChannel();
      const buffer = this._prepareContent(content);

      // Prepare publish options with deduplication support
      const publishOptions = {
        persistent: true,
        ...options,
      };

      // Add deduplication ID to headers if provided
      if (options.deduplicationId) {
        publishOptions.messageId = options.deduplicationId;
        publishOptions.headers = {
          ...(publishOptions.headers || {}),
          'x-deduplication-id': options.deduplicationId,
        };
      }

      const result = channel.publish(exchange, routingKey, buffer, publishOptions);

      if (!result) {
        logger.warn(
          `Channel write buffer is full when publishing to ${exchange} with routing key ${routingKey}`
        );
        // Wait for drain event if buffer is full
        await new Promise(resolve => {
          channel.once('drain', resolve);
        });
      }

      return result;
    } catch (error) {
      logger.error(
        `Error publishing to exchange ${exchange} with routing key ${routingKey}:`,
        error
      );
      throw error;
    }
  }

  /**
   * Prepare content for publishing
   * @param {Buffer|string|Object} content - Content to prepare
   * @returns {Buffer} - Prepared content as Buffer
   * @private
   */
  _prepareContent(content) {
    if (Buffer.isBuffer(content)) {
      return content;
    }

    if (typeof content === 'string') {
      return Buffer.from(content);
    }

    return Buffer.from(JSON.stringify(content));
  }

  /**
   * Send a message directly to a queue with optional deduplication ID
   * @param {string} queue - Queue name
   * @param {Buffer|string|Object} content - Message content
   * @param {Object} options - Publish options
   * @param {string} [options.deduplicationId] - Optional deduplication ID
   * @returns {Promise<boolean>} - Success indicator
   */
  async sendToQueue(queue, content, options = {}) {
    await this._ensureConnected();

    try {
      await this.assertQueue(queue);
      const channel = await this._getPublishChannel();
      const buffer = this._prepareContent(content);

      // Prepare send options with deduplication support
      const sendOptions = {
        persistent: true,
        ...options,
      };

      // Add deduplication ID to headers if provided
      if (options.deduplicationId) {
        sendOptions.messageId = options.deduplicationId;
        sendOptions.headers = {
          ...(sendOptions.headers || {}),
          'x-deduplication-id': options.deduplicationId,
        };
      }

      const result = channel.sendToQueue(queue, buffer, sendOptions);

      if (!result) {
        logger.warn(`Channel write buffer is full when sending to queue ${queue}`);
        // Wait for drain event if buffer is full
        await new Promise(resolve => {
          channel.once('drain', resolve);
        });
      }

      return result;
    } catch (error) {
      logger.error(`Error sending to queue ${queue}:`, error);
      throw error;
    }
  }

  /**
   * Assert that a queue exists (create if it doesn't)
   * @param {string} queue - Queue name
   * @param {Object} options - Queue options
   * @returns {Promise<Object>} - Queue details
   */
  async assertQueue(queue, options = {}) {
    await this._ensureConnected();

    try {
      const channel = await this._getChannelForQueue(queue);
      const queueOptions = {
        ...this._config.defaultQueueOptions,
        ...options,
      };

      const queueInfo = await channel.assertQueue(queue, queueOptions);
      logger.debug(`Asserted queue ${queue}`);
      return queueInfo;
    } catch (error) {
      logger.error(`Error asserting queue ${queue}:`, error);
      throw error;
    }
  }

  /**
   * Delete a queue
   * @param {string} queue - Queue name
   * @param {Object} options - Delete options
   * @returns {Promise<Object>} - Delete result
   */
  async deleteQueue(queue, options = {}) {
    await this._ensureConnected();

    try {
      const channel = await this._getChannelForQueue(queue);

      // Remove from bound queues
      this._boundQueues.delete(queue);

      // Cancel any consumers
      if (this._consumers.has(queue)) {
        await this.cancelConsumer(queue);
      }

      return await channel.deleteQueue(queue, options);
    } catch (error) {
      logger.error(`Error deleting queue ${queue}:`, error);
      throw error;
    }
  }

  /**
   * Assert that an exchange exists (create if it doesn't)
   * @param {string} exchange - Exchange name
   * @param {string} type - Exchange type (direct, topic, fanout, headers)
   * @param {Object} options - Exchange options
   * @returns {Promise<Object>} - Exchange details
   */
  async assertExchange(exchange, type, options = {}) {
    if (!this._exchangeTypes.has(type)) {
      throw new Error(
        `Invalid exchange type: ${type}. Supported types: ${Array.from(this._exchangeTypes).join(', ')}`
      );
    }

    await this._ensureConnected();

    try {
      const channel = await this._getPublishChannel();
      const exchangeOptions = {
        ...this._config.defaultExchangeOptions,
        ...options,
      };

      const result = await channel.assertExchange(exchange, type, exchangeOptions);
      this._declaredExchanges.add(exchange);
      logger.debug(`Asserted ${type} exchange ${exchange}`);
      return result;
    } catch (error) {
      logger.error(`Error asserting exchange ${exchange}:`, error);
      throw error;
    }
  }

  /**
   * Delete an exchange
   * @param {string} exchange - Exchange name
   * @param {Object} options - Delete options
   * @returns {Promise<Object>} - Delete result
   */
  async deleteExchange(exchange, options = {}) {
    await this._ensureConnected();

    try {
      const channel = await this._getPublishChannel();

      // Remove from declared exchanges
      this._declaredExchanges.delete(exchange);

      // Remove related bindings
      for (const [queue, bindings] of this._boundQueues.entries()) {
        const updatedBindings = new Set();
        for (const binding of bindings) {
          if (binding.exchange !== exchange) {
            updatedBindings.add(binding);
          }
        }

        if (updatedBindings.size > 0) {
          this._boundQueues.set(queue, updatedBindings);
        } else {
          this._boundQueues.delete(queue);
        }
      }

      return await channel.deleteExchange(exchange, options);
    } catch (error) {
      logger.error(`Error deleting exchange ${exchange}:`, error);
      throw error;
    }
  }

  /**
   * Bind a queue to an exchange with a routing pattern
   * @param {string} queue - Queue name
   * @param {string} exchange - Exchange name
   * @param {string} pattern - Binding pattern/routing key
   * @param {Object} args - Additional arguments
   * @returns {Promise<Object>} - Binding result
   */
  async bindQueue(queue, exchange, pattern, args = {}) {
    await this._ensureConnected();

    try {
      // Make sure the queue exists
      await this.assertQueue(queue);

      // Default to topic exchange type if not explicitly created yet
      if (!this._declaredExchanges.has(exchange)) {
        await this.assertExchange(exchange, this._config.defaultExchangeType);
      }

      const channel = await this._getChannelForQueue(queue);

      // Track the binding for reconnection purposes
      if (!this._boundQueues.has(queue)) {
        this._boundQueues.set(queue, new Set());
      }

      this._boundQueues.get(queue).add({
        exchange,
        pattern,
      });

      const result = await channel.bindQueue(queue, exchange, pattern, args);
      logger.debug(`Bound queue ${queue} to exchange ${exchange} with pattern ${pattern}`);
      return result;
    } catch (error) {
      logger.error(
        `Error binding queue ${queue} to exchange ${exchange} with pattern ${pattern}:`,
        error
      );
      throw error;
    }
  }

  /**
   * Unbind a queue from an exchange
   * @param {string} queue - Queue name
   * @param {string} exchange - Exchange name
   * @param {string} pattern - Binding pattern/routing key
   * @param {Object} args - Additional arguments
   * @returns {Promise<Object>} - Unbinding result
   */
  async unbindQueue(queue, exchange, pattern, args = {}) {
    await this._ensureConnected();

    try {
      const channel = await this._getChannelForQueue(queue);

      // Remove binding from tracking
      if (this._boundQueues.has(queue)) {
        const bindings = this._boundQueues.get(queue);
        bindings.forEach(binding => {
          if (binding.exchange === exchange && binding.pattern === pattern) {
            bindings.delete(binding);
          }
        });

        if (bindings.size === 0) {
          this._boundQueues.delete(queue);
        }
      }

      return await channel.unbindQueue(queue, exchange, pattern, args);
    } catch (error) {
      logger.error(
        `Error unbinding queue ${queue} from exchange ${exchange} with pattern ${pattern}:`,
        error
      );
      throw error;
    }
  }

  /**
   * Create a sharded queue set for distributed processing
   * @param {string} baseName - Base name for the queues
   * @param {number} count - Number of shards to create
   * @param {Object} options - Queue options
   * @returns {Promise<Array<string>>} - Array of created queue names
   */
  async createShardedQueues(baseName, count, options = {}) {
    await this._ensureConnected();

    const queueNames = [];

    try {
      for (let i = 0; i < count; i++) {
        const queueName = `${baseName}.${i}`;
        await this.assertQueue(queueName, options);
        queueNames.push(queueName);
      }

      logger.info(`Created ${count} sharded queues with base name ${baseName}`);
      return queueNames;
    } catch (error) {
      logger.error(`Error creating sharded queues for ${baseName}:`, error);
      throw error;
    }
  }

  /**
   * Publish a message to a sharded queue based on a consistent hash
   * @param {string} baseName - Base name of the sharded queues
   * @param {number} count - Number of shards
   * @param {string} routingKey - Key for determining shard (e.g., user ID)
   * @param {Buffer|string|Object} content - Message content
   * @param {Object} options - Publish options
   * @returns {Promise<boolean>} - Success indicator
   */
  async publishToShard(baseName, count, routingKey, content, options = {}) {
    await this._ensureConnected();

    try {
      // Simple hash function for consistent routing
      const hash = this._calculateHash(routingKey);
      const shardIndex = hash % count;
      const queueName = `${baseName}.${shardIndex}`;

      // Ensure queue exists
      await this.assertQueue(queueName);

      // Send to the specific queue
      return await this.sendToQueue(queueName, content, options);
    } catch (error) {
      logger.error(`Error publishing to shard for ${baseName}/${routingKey}:`, error);
      throw error;
    }
  }

  /**
   * Calculate a hash for consistent routing
   * @param {string} str - String to hash
   * @returns {number} - Hash value
   * @private
   */
  _calculateHash(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return Math.abs(hash);
  }

  /**
   * Cancel a consumer
   * @param {string} queue - Queue name for the consumer
   * @returns {Promise<void>}
   */
  async cancelConsumer(queue) {
    if (!this._consumers.has(queue)) {
      logger.warn(`No consumer found for queue ${queue}`);
      return;
    }

    await this._ensureConnected();

    try {
      const { consumerTag } = this._consumers.get(queue);
      const channel = await this._getChannelForQueue(queue);
      await channel.cancel(consumerTag);
      this._consumers.delete(queue);
      logger.info(`Cancelled consumer for queue ${queue}`);
    } catch (error) {
      logger.error(`Error cancelling consumer for queue ${queue}:`, error);
      throw error;
    }
  }

  /**
   * Close the RabbitMQ connection
   * @returns {Promise<void>}
   */
  async close() {
    if (!this._connected) {
      logger.debug('RabbitMQ connection already closed');
      return;
    }

    try {
      // Cancel all consumers
      for (const queue of this._consumers.keys()) {
        try {
          await this.cancelConsumer(queue);
        } catch (error) {
          logger.warn(`Error cancelling consumer for queue ${queue}:`, error);
        }
      }

      await this._cleanupExistingConnection();
      this._connected = false;
      logger.info('RabbitMQ client closed successfully');
    } catch (error) {
      logger.error('Error closing RabbitMQ client:', error);
      throw error;
    }
  }

  /**
   * Check RabbitMQ connection health
   * @returns {Promise<Object>} - Health status object
   */
  async healthCheck() {
    try {
      if (!this._client || !this._connected || this._reconnecting) {
        return { status: 'error', message: 'Connection not established or reconnecting' };
      }

      // Try to get the publish channel to verify connection is working
      await this._getPublishChannel();

      // Check if we can create and delete a temporary queue
      const channel = await this._getPublishChannel();
      const testQueue = `health-check-${Date.now()}`;
      await channel.assertQueue(testQueue, { durable: false, autoDelete: true });
      await channel.deleteQueue(testQueue);

      return { status: 'ok', message: 'RabbitMQ connection is healthy' };
    } catch (error) {
      logger.error('Health check failed:', error);
      return { status: 'error', message: error.message };
    }
  }
}
