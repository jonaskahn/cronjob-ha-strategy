import getLogger from '../../src/utils/logger.js';
import rabbitMQClient from '../../src/client/rabbitMQClient.js';
import messageTracker from '../../src/core/messageTracker.js';
import { StateStorage } from '../../src/core/storage.js';
import coordinator from '../../src/core/coordinator.js';
import queueManager from '../../src/core/queueManager.js';
import { Worker } from '../../src/worker.js';
import consumer from '../../src/core/consumer.js';

const logger = getLogger('run-test');
const QUEUES = ['customer', 'event', 'email', 'users'];
const RECORDS_PER_QUEUE = 100;

/**
 * Comprehensive test application to test the full flow of the system
 */
class TestApplication {
  constructor() {
    this._running = false;
    this._queues = QUEUES;
    this._messageCount = RECORDS_PER_QUEUE;
    this._messageTracker = messageTracker;
    this._stateStorage = new StateStorage(null, { useInMemory: true, forceNew: true });
    this._coordinator = coordinator;
    this._queueManager = queueManager;
    this._consumer = consumer;
    this._publisher = { publishMessage: this._publishMessage.bind(this) };
    this._worker = new Worker();
    this._worker._stateStorage = this._stateStorage;
    this._worker._consumer = this._consumer;
    this._worker._messageTracker = this._messageTracker;

    logger.info('Test application initialized');
  }

  /**
   * Start the test application
   * @returns {Promise<void>}
   */
  async start() {
    logger.info('Starting test application...');

    try {
      // Connect to services
      await this._connectServices();

      // Start the worker
      await this._worker.start();

      // Register test handlers
      await this._registerTestHandlers();

      this._running = true;
      logger.info('Test application started');
    } catch (error) {
      logger.error('Failed to start test application:', error);
      throw error;
    }
  }

  /**
   * Register test handlers for processing messages
   * @returns {Promise<void>}
   */
  async _registerTestHandlers() {
    logger.info('Registering test handlers...');

    // Create a simple handler function that logs and returns success
    const createTestHandler = (queueName, processingTime = 100) => {
      return async (message, metadata) => {
        logger.info(
          `Processing ${queueName} message: ${metadata.messageId} (${metadata.currentState} -> ${metadata.processingState} -> ${metadata.nextState})`
        );

        // Simulate processing time
        await new Promise(resolve => setTimeout(resolve, processingTime));

        logger.info(`Completed ${queueName} message: ${metadata.messageId}`);
        return true;
      };
    };

    // Register handlers for each queue with appropriate state transitions
    this._consumer.registerStateHandler(
      'initial',
      'processing',
      'completed',
      createTestHandler('customer', 200)
    );

    this._consumer.registerStateHandler(
      'new_event',
      'processing_event',
      'event_processed',
      createTestHandler('event', 150)
    );

    this._consumer.registerStateHandler(
      'email_queued',
      'sending_email',
      'email_sent',
      createTestHandler('email', 250)
    );

    this._consumer.registerStateHandler(
      'user_action',
      'processing_user_action',
      'user_action_completed',
      createTestHandler('user', 100)
    );

    logger.info('Test handlers registered');
  }

  /**
   * Initialize storage with test data
   * @returns {Promise<void>}
   */
  async initializeStorage() {
    logger.info(`Initializing storage with ${this._messageCount} records per queue...`);

    // Initialize state storage
    await this._stateStorage.initializeTable();

    // Generate and publish test messages to each queue
    const publishPromises = [];

    for (const queue of this._queues) {
      logger.info(`Initializing ${this._messageCount} records for queue: ${queue}`);

      for (let i = 0; i < this._messageCount; i++) {
        // Create message based on queue type
        const message = this._createMessage(queue, i);

        // Publish the message
        publishPromises.push(this._publisher.publishMessage(queue, message));

        // Add a small delay every 10 messages to avoid overwhelming the system
        if (i % 10 === 0 && i > 0) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }
    }

    await Promise.all(publishPromises);
    logger.info('Storage initialization completed');
  }

  /**
   * Consume messages from all queues
   * @returns {Promise<void>}
   */
  async consumeMessages() {
    logger.info('Starting to consume messages from all queues...');

    // Set up metrics
    const metrics = {
      processed: 0,
      failed: 0,
      startTime: Date.now(),
      endTime: null,
      processingTimes: {},
      queueMetrics: {},
    };

    for (const queue of this._queues) {
      metrics.processingTimes[queue] = [];
      metrics.queueMetrics[queue] = { processed: 0, failed: 0 };
    }

    // Start consuming messages
    await this._consumer.startConsuming();

    // Set up monitoring interval
    const monitorInterval = setInterval(() => {
      logger.info(`Progress: ${metrics.processed} processed, ${metrics.failed} failed`);

      for (const queue of this._queues) {
        const qm = metrics.queueMetrics[queue];
        logger.info(`Queue ${queue}: ${qm.processed} processed, ${qm.failed} failed`);
      }
    }, 5000);

    // Set up completion check interval
    const completionInterval = setInterval(async () => {
      let allQueuesEmpty = true;

      for (const queue of this._queues) {
        const queueInfo = await this._queueManager.getQueueInfo(queue);
        if (queueInfo.messageCount > 0) {
          allQueuesEmpty = false;
          break;
        }
      }

      if (allQueuesEmpty) {
        clearInterval(completionInterval);
        clearInterval(monitorInterval);

        metrics.endTime = Date.now();
        await this._reportResults(metrics);

        await this.shutdown();
      }
    }, 2000);
  }

  /**
   * Report test results
   * @param {Object} metrics - Test metrics
   * @returns {Promise<void>}
   */
  async _reportResults(metrics) {
    const totalTime = (metrics.endTime - metrics.startTime) / 1000;
    const messagesPerSecond = metrics.processed / totalTime;

    logger.info('=== Test Results ===');
    logger.info(`Total messages processed: ${metrics.processed}`);
    logger.info(`Total messages failed: ${metrics.failed}`);
    logger.info(`Total processing time: ${totalTime.toFixed(2)} seconds`);
    logger.info(`Messages per second: ${messagesPerSecond.toFixed(2)}`);

    for (const queue of this._queues) {
      const qm = metrics.queueMetrics[queue];
      const times = metrics.processingTimes[queue];
      const avgTime =
        times.length > 0 ? times.reduce((sum, time) => sum + time, 0) / times.length : 0;

      logger.info(`=== Queue: ${queue} ===`);
      logger.info(`Messages processed: ${qm.processed}`);
      logger.info(`Messages failed: ${qm.failed}`);
      logger.info(`Average processing time: ${avgTime.toFixed(2)} ms`);
    }
  }

  /**
   * Shutdown the test application
   * @returns {Promise<void>}
   */
  async shutdown() {
    if (!this._running) {
      return;
    }

    this._running = false;
    logger.info('Shutting down test application...');

    try {
      // Stop consumer
      await this._consumer.stopConsuming();

      // Shutdown the worker
      await this._worker.shutdown();

      logger.info('Test application shutdown complete');
    } catch (error) {
      logger.error('Error during test application shutdown:', error);
    }
  }

  /**
   * Connect to services needed for testing
   * @returns {Promise<void>}
   */
  async _connectServices() {
    try {
      // Connect to RabbitMQ
      logger.info('Connecting to RabbitMQ...');
      await rabbitMQClient.connect();

      // Skip Redis and etcd connection checks for testing
      logger.info('Skipping Redis and etcd connection checks for testing purposes');

      // Initialize the storage for in-memory mode
      await this._stateStorage.initializeTable();

      // Register with coordinator (mocked)
      await this._coordinator.register();

      // Explicitly assign all queues to this consumer for testing
      await this._coordinator.assignQueues(this._queues);
      logger.info(`Assigned test queues to coordinator: ${this._queues.join(', ')}`);

      // Initialize queue manager
      await this._queueManager.initialize();

      // Ensure all queues exist
      for (const queue of this._queues) {
        await rabbitMQClient.assertQueue(queue);
        logger.info(`Ensured queue exists: ${queue}`);
      }

      // Initialize consumer
      await this._consumer.initialize();

      return true;
    } catch (error) {
      logger.error('Failed to connect to services:', error);
      throw new Error(`Service connection failed: ${error.message}`);
    }
  }

  /**
   * Create a test message based on queue type
   * @param {string} queue - Queue name
   * @param {number} index - Message index
   * @returns {Object} - Message object
   * @private
   */
  _createMessage(queue, index) {
    const baseMessage = {
      id: `${queue}-${Date.now()}-${index}`,
      timestamp: new Date().toISOString(),
      priority: Math.floor(Math.random() * 10),
      testIndex: index,
    };

    // Set initial state based on queue
    let state = 'initial';
    if (queue === 'event') state = 'new_event';
    if (queue === 'email') state = 'email_queued';
    if (queue === 'users') state = 'user_action';

    // Add queue-specific data
    const data = {};

    switch (queue) {
      case 'customer':
        data.customerId = `cust-${Math.floor(Math.random() * 1000)}`;
        data.action = ['create', 'update', 'delete'][Math.floor(Math.random() * 3)];
        data.attributes = {
          name: `Customer ${index}`,
          email: `customer${index}@example.com`,
          segments: ['new', 'active', 'vip'][Math.floor(Math.random() * 3)],
        };
        break;

      case 'event':
        data.eventType = ['click', 'pageview', 'purchase', 'signup'][Math.floor(Math.random() * 4)];
        data.userId = `user-${Math.floor(Math.random() * 1000)}`;
        data.timestamp = Date.now();
        data.metadata = {
          url: `https://example.com/page${index}`,
          referrer: `https://source${Math.floor(Math.random() * 10)}.com`,
          deviceType: ['mobile', 'desktop', 'tablet'][Math.floor(Math.random() * 3)],
        };
        break;

      case 'email':
        data.recipient = `recipient${index}@example.com`;
        data.subject = `Test Email Subject ${index}`;
        data.templateId = `template-${Math.floor(Math.random() * 5)}`;
        data.variables = {
          name: `User ${index}`,
          content: `Custom content for email ${index}`,
        };
        break;

      case 'users':
        data.userId = `user-${Math.floor(Math.random() * 1000)}`;
        data.operation = ['create', 'update', 'delete', 'login', 'logout'][
          Math.floor(Math.random() * 5)
        ];
        data.userData = {
          email: `user${index}@example.com`,
          role: ['admin', 'user', 'guest'][Math.floor(Math.random() * 3)],
          lastLogin: new Date().toISOString(),
        };
        break;
    }

    return {
      ...baseMessage,
      state,
      data,
    };
  }

  /**
   * Publish a message to a queue
   * @param {string} queue - Queue name
   * @param {object} message - Message to publish
   * @returns {Promise<boolean>} - Success indicator
   * @private
   */
  async _publishMessage(queue, message) {
    try {
      let state = 'initial';
      if (queue === 'event') state = 'new_event';
      if (queue === 'email') state = 'email_queued';
      if (queue === 'users') state = 'user_action';

      // Determine next state based on current state
      let processingState = 'processing';
      let nextState = 'completed';

      if (state === 'new_event') {
        processingState = 'processing_event';
        nextState = 'event_processed';
      } else if (state === 'email_queued') {
        processingState = 'sending_email';
        nextState = 'email_sent';
      } else if (state === 'user_action') {
        processingState = 'processing_user_action';
        nextState = 'user_action_completed';
      }

      const messageWithState = {
        ...message,
        state,
      };

      // Add headers required for message processing
      const headers = {
        'x-message-id': message.id,
        'x-current-state': state,
        'x-processing-state': processingState,
        'x-next-state': nextState,
        'x-published-at': Date.now(),
      };

      const publishOptions = {
        headers,
        messageId: message.id,
        persistent: true,
      };

      // Publish directly using RabbitMQ client
      await rabbitMQClient.sendToQueue(queue, messageWithState, publishOptions);
      logger.info(`Published message ${message.id} to ${queue} with state: ${state}`);

      return true;
    } catch (error) {
      logger.error(`Error publishing message to ${queue}:`, error);
      return false;
    }
  }
}

// Create and run the test application
const testApp = new TestApplication();

const run = async () => {
  try {
    // Start the application
    await testApp.start();

    // Initialize storage with test data
    await testApp.initializeStorage();

    // Start consuming messages
    await testApp.consumeMessages();

    logger.info('Test application running. Press Ctrl+C to stop.');
  } catch (error) {
    logger.error('Error in test application:', error);
    await testApp.shutdown();
    process.exit(1);
  }
};

// Handle process termination
process.on('SIGTERM', async () => {
  logger.info('Received SIGTERM. Shutting down...');
  await testApp.shutdown();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('Received SIGINT. Shutting down...');
  await testApp.shutdown();
  process.exit(0);
});

// Run the test application
run();
