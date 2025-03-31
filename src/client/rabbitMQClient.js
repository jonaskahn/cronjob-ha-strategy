import amqp from 'amqplib';
import getLogger from '../utils/logger.js';
// Import the config
import appConfig from '../utils/appConfig.js';

const logger = getLogger('client/RabbitMQClient');

class RabbitMQClient {
  constructor(config) {
    this._config = config;
    this._connection = null;
    this._channel = null;
    this._publishChannel = null;
    this._consumeChannel = null;
    this._isConnected = false;
    this._exchangeTypes = new Map(); // Track exchange types for consistency
    this._activeConsumers = new Map(); // Track consumer tags by queue name

    // Default to direct exchange type if not specified
    this._defaultExchangeType = config.rabbitmq?.defaultExchangeType || 'direct';

    logger.info(
      `RabbitMQ client initialized with default exchange type: ${this._defaultExchangeType}`
    );
  }

  async connect() {
    try {
      if (this._connection && this._isConnected) {
        return;
      }

      const url = `amqp://${this._config.rabbitmq.connectionUrl}`;
      logger.info(`Connecting to RabbitMQ at ${url}`);

      this._connection = await amqp.connect(url, {
        heartbeat: this._config.rabbitmq.heartbeat / 1000, // Convert to seconds
      });

      this._connection.on('error', err => {
        logger.error('RabbitMQ connection error:', err);
        this._isConnected = false;
        this._reconnect();
      });

      this._connection.on('close', () => {
        logger.info('RabbitMQ connection closed');
        this._isConnected = false;
        this._reconnect();
      });

      // Create channels
      this._channel = await this._connection.createChannel();
      this._publishChannel = await this._connection.createChannel();
      this._consumeChannel = await this._connection.createChannel();

      // Set up error handlers for publish channel
      this._publishChannel.on('error', err => {
        logger.error('Publish channel error:', err);
      });

      this._publishChannel.on('close', () => {
        logger.info('Publish channel closed');
      });

      // Set up error handlers for consume channel
      this._consumeChannel.on('error', err => {
        logger.error('Consume channel error:', err);
      });

      this._consumeChannel.on('close', () => {
        logger.info('Consume channel closed');
      });

      this._isConnected = true;
      logger.info('Successfully connected to RabbitMQ');

      return this;
    } catch (error) {
      logger.error('Failed to connect to RabbitMQ:', error);
      this._isConnected = false;
      // Retry connection
      setTimeout(() => this._reconnect(), 5000);
      throw error;
    }
  }

  async _reconnect() {
    if (this._isConnected) return;

    try {
      logger.info('Attempting to reconnect to RabbitMQ...');
      await this.connect();
    } catch (error) {
      logger.error('Error during reconnection attempt:', error);
      // Schedule another reconnection attempt
      setTimeout(() => this._reconnect(), 5000);
    }
  }

  async assertExchange(exchangeName, type = null, options = {}) {
    await this._ensureConnected();

    // Use default type if not specified
    if (!type) {
      type = this._defaultExchangeType;
    }

    // Use cached exchange type if available to avoid mismatches
    if (this._exchangeTypes.has(exchangeName)) {
      const existingType = this._exchangeTypes.get(exchangeName);
      if (existingType !== type) {
        logger.warn(
          `Exchange type mismatch for ${exchangeName}: requested ${type}, using existing ${existingType}`
        );
        type = existingType;
      }
    }

    try {
      await this._publishChannel.assertExchange(exchangeName, type, {
        ...this._config.rabbitmq.defaultExchangeOptions,
        ...options,
      });
      this._exchangeTypes.set(exchangeName, type);
      logger.debug(`Asserted exchange: ${exchangeName} (${type})`);
    } catch (error) {
      // Handle exchange type mismatch
      if (
        error.message &&
        error.message.includes('PRECONDITION_FAILED') &&
        error.message.includes("inequivalent arg 'type'")
      ) {
        const match = error.message.match(/received '(\w+)' but current is '(\w+)'/);
        if (match && match[2]) {
          const actualType = match[2];
          logger.warn(`Exchange ${exchangeName} exists with different type: ${actualType}`);

          // Update our cache with the actual type
          this._exchangeTypes.set(exchangeName, actualType);

          // Retry with the correct type
          try {
            await this._publishChannel.assertExchange(exchangeName, actualType, {
              ...this._config.rabbitmq.defaultExchangeOptions,
              ...options,
            });
            logger.info(`Successfully asserted exchange ${exchangeName} with type ${actualType}`);
            return;
          } catch (retryError) {
            logger.error(`Failed to assert exchange with actual type:`, retryError);
            throw retryError;
          }
        }
      }

      logger.error(`Error asserting exchange ${exchangeName}:`, error);
      throw error;
    }
  }

  async assertQueue(queueName, options = {}) {
    await this._ensureConnected();
    try {
      const queueOptions = {
        ...this._config.rabbitmq.defaultQueueOptions,
        ...options,
      };

      await this._consumeChannel.assertQueue(queueName, queueOptions);
      logger.debug(`Asserted queue: ${queueName}`);
    } catch (error) {
      logger.error(`Error asserting queue ${queueName}:`, error);

      // Retry on connection errors
      if (
        error.message &&
        (error.message.includes('Connection closed') || error.message.includes('Channel closed'))
      ) {
        logger.info(`Retrying queue assertion for ${queueName}...`);
        await new Promise(resolve => setTimeout(resolve, 1000));

        await this._ensureConnected();
        await this._consumeChannel.assertQueue(queueName, {
          ...this._config.rabbitmq.defaultQueueOptions,
          ...options,
        });
      } else {
        throw error;
      }
    }
  }

  async bindQueue(queueName, exchangeName, routingKey) {
    await this._ensureConnected();
    try {
      await this._consumeChannel.bindQueue(queueName, exchangeName, routingKey);
      logger.debug(
        `Bound queue ${queueName} to exchange ${exchangeName} with routing key ${routingKey}`
      );
    } catch (error) {
      logger.error(`Error binding queue ${queueName} to exchange ${exchangeName}:`, error);
      throw error;
    }
  }

  async publish(exchangeName, routingKey, content, options = {}) {
    await this._ensureConnected();

    // Convert content to Buffer
    let messageContent;
    if (Buffer.isBuffer(content)) {
      messageContent = content;
    } else if (typeof content === 'string') {
      messageContent = Buffer.from(content);
      if (!options.contentType) {
        options.contentType = 'text/plain';
      }
    } else if (typeof content === 'object') {
      messageContent = Buffer.from(JSON.stringify(content));
      if (!options.contentType) {
        options.contentType = 'application/json';
      }
    } else {
      messageContent = Buffer.from(String(content));
    }

    try {
      // Ensure channel is not closed
      if (this._publishChannel.closed) {
        logger.warn('Publish channel closed, creating new channel');
        this._publishChannel = await this._connection.createChannel();
      }

      // Get the exchange type - use cached value if available
      const exchangeType = this._exchangeTypes.get(exchangeName) || this._defaultExchangeType;

      // Ensure exchange exists before publishing
      await this.assertExchange(exchangeName, exchangeType);

      // Publish the message
      const publishResult = this._publishChannel.publish(exchangeName, routingKey, messageContent, {
        persistent: true,
        ...options,
      });

      // Handle back pressure
      if (!publishResult) {
        logger.warn(`Buffer full when publishing to ${exchangeName}, waiting for drain`);
        await new Promise(resolve => {
          this._publishChannel.once('drain', resolve);
        });
      }

      return true;
    } catch (error) {
      logger.error(
        `Error publishing to exchange ${exchangeName} with routing key ${routingKey}:`,
        error
      );

      // Try to recreate channel on error
      if (
        error.message &&
        (error.message.includes('Channel closed') || error.message.includes('Connection closed'))
      ) {
        await this._ensureConnected();
        try {
          this._publishChannel = await this._connection.createChannel();
        } catch (channelError) {
          logger.error('Failed to recreate publish channel:', channelError);
        }
      }

      throw error;
    }
  }

  async sendToQueue(queueName, content, options = {}) {
    await this._ensureConnected();

    // Convert content to Buffer
    let messageContent;
    if (Buffer.isBuffer(content)) {
      messageContent = content;
    } else if (typeof content === 'string') {
      messageContent = Buffer.from(content);
      if (!options.contentType) {
        options.contentType = 'text/plain';
      }
    } else if (typeof content === 'object') {
      messageContent = Buffer.from(JSON.stringify(content));
      if (!options.contentType) {
        options.contentType = 'application/json';
      }
    } else {
      messageContent = Buffer.from(String(content));
    }

    try {
      // Ensure queue exists
      await this.assertQueue(queueName);

      // Send to queue
      const sendResult = this._publishChannel.sendToQueue(queueName, messageContent, {
        persistent: true,
        ...options,
      });

      // Handle back pressure
      if (!sendResult) {
        logger.warn(`Buffer full when sending to queue ${queueName}, waiting for drain`);
        await new Promise(resolve => {
          this._publishChannel.once('drain', resolve);
        });
      }

      return true;
    } catch (error) {
      logger.error(`Error sending to queue ${queueName}:`, error);
      throw error;
    }
  }

  async consume(queueName, callback, options = {}) {
    await this._ensureConnected();

    try {
      // Set prefetch count
      const prefetch = options.prefetch || this._config.rabbitmq.prefetch;
      await this._consumeChannel.prefetch(prefetch);

      // Ensure queue exists
      await this.assertQueue(queueName);

      // Start consuming
      const { consumerTag } = await this._consumeChannel.consume(
        queueName,
        async msg => {
          if (!msg) {
            logger.warn(`Consumer cancelled by RabbitMQ for queue ${queueName}`);
            this._activeConsumers.delete(queueName);
            return;
          }

          try {
            // Add helper methods to the message
            msg.ack = () => this._consumeChannel.ack(msg);
            msg.nack = (requeue = true) => this._consumeChannel.nack(msg, false, requeue);
            msg.reject = (requeue = true) => this._consumeChannel.reject(msg, requeue);

            // Add json helper method
            msg.json = () => {
              try {
                const content = msg.content.toString();
                return JSON.parse(content);
              } catch (error) {
                logger.error(
                  `Error parsing message content as JSON from queue ${queueName}:`,
                  error
                );
                return null;
              }
            };

            // Process the message
            await callback(msg);
          } catch (error) {
            logger.error(`Error processing message from queue ${queueName}:`, error);
            try {
              // Reject the message and don't requeue on processing error
              this._consumeChannel.reject(msg, false);
            } catch (rejectError) {
              logger.error(`Error rejecting message:`, rejectError);
            }
          }
        },
        options
      );

      // Track active consumer
      this._activeConsumers.set(queueName, consumerTag);

      logger.info(`Started consuming from queue ${queueName} with consumer tag ${consumerTag}`);
      return consumerTag;
    } catch (error) {
      logger.error(`Error consuming from queue ${queueName}:`, error);
      throw error;
    }
  }

  async cancelConsumer(consumerTag) {
    await this._ensureConnected();

    try {
      if (consumerTag) {
        await this._consumeChannel.cancel(consumerTag);

        // Remove from tracked consumers
        for (const [queueName, tag] of this._activeConsumers.entries()) {
          if (tag === consumerTag) {
            this._activeConsumers.delete(queueName);
            break;
          }
        }

        logger.info(`Cancelled consumer with tag ${consumerTag}`);
        return true;
      }

      logger.warn(`No consumer tag provided`);
      return false;
    } catch (error) {
      logger.error(`Error cancelling consumer ${consumerTag}:`, error);
      throw error;
    }
  }

  async _ensureConnected() {
    if (!this._isConnected || !this._connection || !this._channel) {
      await this.connect();
    }

    // Also check if channels are closed and recreate if necessary
    if (this._publishChannel.closed) {
      this._publishChannel = await this._connection.createChannel();
    }

    if (this._consumeChannel.closed) {
      this._consumeChannel = await this._connection.createChannel();
    }
  }

  async close() {
    try {
      if (this._connection) {
        // Cancel all consumers
        for (const [queueName, consumerTag] of this._activeConsumers.entries()) {
          try {
            await this.cancelConsumer(consumerTag);
          } catch (error) {
            logger.warn(`Error cancelling consumer for queue ${queueName}:`, error);
          }
        }

        // Close the connection
        await this._connection.close();
        this._connection = null;
        this._channel = null;
        this._publishChannel = null;
        this._consumeChannel = null;
        this._isConnected = false;
        this._activeConsumers.clear();
        logger.info('RabbitMQ connection closed');
      }
    } catch (error) {
      logger.error('Error closing RabbitMQ connection:', error);
      throw error;
    }
  }
}

const rabbitMQClient = new RabbitMQClient(appConfig);
export default rabbitMQClient;
