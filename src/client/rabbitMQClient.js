import getLogger from '../utils/logger.js';
import amqp from 'amqplib';

const logger = getLogger('client/RabbitMQClient');

class RabbitMQClient {
  constructor() {
    this._client = null;
    this._reconnecting = false;
    this._heartbeat = Number(process.env.CHJS_RABBITMQ_CLUSTER_HEARTBEAT) || 5000;
    this._channels = new Map();
    this._consumers = new Map();
  }

  async connect() {
    try {
      logger.debug('Starting RabbitMQ connection');
      const connectionUrl = process.env.CHJS_RABBITMQ_CLUSTER_HOST || 'admin:admin@localhost:5672';
      this._client = await amqp.connect(`amqp://${connectionUrl}`, {
        heartbeat: this._heartbeat,
      });
      this._client.on('error', async err => {
        logger.error('RabbitMQ connection error:', err);
        await this.#reconnect();
      });
      logger.info('RabbitMQ connection established');
    } catch (error) {
      logger.error('Failed to connect to RabbitMQ:', error);
      throw error;
    }
  }

  async #reconnect() {
    if (this._reconnecting) return;

    this._reconnecting = true;
    logger.info('Initiating RabbitMQ reconnection sequence');

    try {
      await this.#cleanupExistingConnection();
      await this.#reconnectWithBackoff();
      await this.#restoreConsumers();
      logger.info('RabbitMQ reconnection completed successfully');
    } catch (error) {
      logger.error('RabbitMQ reconnection failed:', error);
    } finally {
      this._reconnecting = false;
    }
  }

  async #cleanupExistingConnection() {
    try {
      for (const [queueName, channel] of this._channels.entries()) {
        try {
          await channel.close();
        } catch (error) {
          logger.warn(`Error closing channel for ${queueName}:`, error);
        }
      }
      this._channels.clear();

      if (this._client) {
        await this._client.close();
        this._client = null;
      }
    } catch (error) {
      logger.warn('Error during connection cleanup:', error);
    }
  }

  async #reconnectWithBackoff(attempt = 3) {
    const MAX_ATTEMPTS = 10;
    const MAX_DELAY_MS = 30_000;
    try {
      await this.connect();
    } catch (error) {
      if (attempt >= MAX_ATTEMPTS) {
        logger.error(`Reconnection failed after ${MAX_ATTEMPTS} attempts`);
        throw error;
      }

      const delayMs = Math.min(1000 * Math.pow(2, attempt), MAX_DELAY_MS);
      logger.info(`Reconnection attempt ${attempt} failed, retrying in ${delayMs}ms`);

      await new Promise(resolve => setTimeout(resolve, delayMs));
      await this.#reconnectWithBackoff(attempt + 1);
    }
  }

  async #restoreConsumers() {
    for (const [queue, consumerInfo] of this._consumers.entries()) {
      try {
        await this.consume(queue, consumerInfo.handler, consumerInfo.options);
        logger.info(`Restored consumer for queue: ${queue}`);
      } catch (error) {
        logger.error(`Failed to restore consumer for queue ${queue}:`, error);
      }
    }
  }

  async listQueues() {
    try {
      return await this.#fetchCurrentQueues();
    } catch (error) {
      logger.error('Error listing queues:', error);
      throw error;
    }
  }

  async #fetchCurrentQueues() {
    const connectionUrl = process.env.CHJS_RABBITMQ_MANAGER_HOST || 'admin:admin@localhost:15672';
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
    return queues.map(queue => ({ name: queue.name }));
  }

  async consume(queue, handler, options = { prefetch: 10 }) {
    try {
      const channel = await this.#getChannelForQueue(queue);
      await channel.prefetch(options.prefetch);
      const consumerTag = await this.#startConsuming(queue, channel, handler);
      this.#registerConsumer(queue, handler, options, consumerTag);
      logger.debug(`Started consuming from queue ${queue}`);
      return consumerTag;
    } catch (error) {
      logger.error(`Error consuming from queue ${queue}:`, error);
      throw error;
    }
  }

  async #getChannelForQueue(queue) {
    if (this._channels.has(queue)) {
      return this._channels.get(queue);
    }
    const channel = await this.#createChannelForQueue(queue);
    this._channels.put(queue, channel);
  }

  async #createChannelForQueue(queue) {
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

  async #startConsuming(queue, channel, handler) {
    const { consumerTag } = await channel.consume(queue, message =>
      this.#handleIncomingMessage(message, channel, handler, queue)
    );
    return consumerTag;
  }

  async #handleIncomingMessage(message, channel, handler, queue) {
    if (!message) return; // Consumer cancelled

    try {
      const enhancedMessage = this.#enhanceMessageWithMethods(message, channel);
      await handler(enhancedMessage, queue);
    } catch (error) {
      logger.error(`Error processing message from ${queue}:`, error);
      await channel.reject(message, false);
    }
  }

  #enhanceMessageWithMethods(message, channel) {
    return {
      ...message,
      content: message.content,
      ack: async () => {
        await channel.ack(message);
      },
      reject: async (requeue = false) => {
        await channel.reject(message, requeue);
      },
    };
  }

  #registerConsumer(queue, handler, options, consumerTag) {
    this._consumers.set(queue, {
      handler,
      options,
      consumerTag,
    });
  }
}

const rabbitMqClient = async () => {
  const client = new RabbitMQClient();
  await client.connect();
  return client;
};

export default rabbitMqClient;
