// src/test-publisher.js
import getLogger from './utils/logger.js';
import config from './utils/config.js';
import rabbitMQClient from './client/rabbitMQClient.js';

const logger = getLogger('test-publisher');

/**
 * A simple test script that publishes messages to the test queues
 */
class TestPublisher {
  constructor() {
    this._config = config;
    this._queues = ['customer', 'event', 'email', 'users'];
  }

  /**
   * Initialize the publisher and connect to RabbitMQ
   */
  async initialize() {
    logger.info('Initializing test publisher...');

    // Connect to RabbitMQ
    await rabbitMQClient.connect();
    logger.info('Connected to RabbitMQ');

    // Make sure the queues exist
    for (const queue of this._queues) {
      await rabbitMQClient.assertQueue(queue);
      logger.info(`Queue ${queue} asserted`);
    }
  }

  /**
   * Publish a test message to a queue
   * @param {string} queue - Queue name
   * @param {Object} data - Message data
   * @param {Object} options - Publishing options
   * @returns {Promise<void>}
   */
  async publishMessage(queue, data, options = {}) {
    const message = {
      id: `msg-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
      timestamp: new Date().toISOString(),
      data,
      ...options,
    };

    let state = 'initial';
    if (queue === 'event') state = 'new_event';
    if (queue === 'email') state = 'email_queued';
    if (queue === 'users') state = 'user_action';

    const messageWithState = {
      ...message,
      state,
    };

    logger.info(`Publishing message to ${queue}: ${message.id}`);
    await rabbitMQClient.publish(queue, messageWithState);
    logger.info(`Message published to ${queue}: ${message.id}`);

    return message.id;
  }

  /**
   * Publish multiple test messages to each queue
   * @param {number} count - Number of messages per queue
   * @returns {Promise<void>}
   */
  async publishTestMessages(count = 2) {
    logger.info(`Publishing ${count} test messages to each queue...`);

    for (const queue of this._queues) {
      for (let i = 0; i < count; i++) {
        const data = {
          test: `Test message ${i + 1} for ${queue}`,
          priority: Math.floor(Math.random() * 10),
        };

        await this.publishMessage(queue, data);

        // Small delay between messages
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }

    logger.info('All test messages published');
  }

  /**
   * Close connections
   */
  async close() {
    await rabbitMQClient.close();
    logger.info('Test publisher closed');
  }
}

// Create and run the test publisher if run directly
if (import.meta.url === import.meta.main) {
  const publisher = new TestPublisher();

  const run = async () => {
    try {
      await publisher.initialize();
      await publisher.publishTestMessages(3);
    } catch (error) {
      logger.error('Error in test publisher:', error);
    } finally {
      await publisher.close();
    }
  };

  run().catch(error => {
    logger.error('Fatal error in test publisher:', error);
    process.exit(1);
  });
}

export default TestPublisher;
