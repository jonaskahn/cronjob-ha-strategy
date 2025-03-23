import getLogger from './utils/logger.js';

const logger = getLogger('test-flow');

// Create mock clients
const mockRabbitMQ = {
  queues: {},
  exchanges: {},

  connect() {
    logger.info('Mock RabbitMQ connected');
    return Promise.resolve();
  },

  close() {
    logger.info('Mock RabbitMQ connection closed');
    return Promise.resolve();
  },

  assertQueue(queue) {
    if (!this.queues[queue]) {
      this.queues[queue] = [];
      logger.info(`Queue "${queue}" created`);
    }
    return Promise.resolve({ queue });
  },

  assertExchange(exchange, type) {
    if (!this.exchanges[exchange]) {
      this.exchanges[exchange] = { type, bindings: {} };
      logger.info(`Exchange "${exchange}" (${type}) created`);
    }
    return Promise.resolve({ exchange });
  },

  bindQueue(queue, exchange, routingKey) {
    if (!this.exchanges[exchange]) {
      this.exchanges[exchange] = { type: 'direct', bindings: {} };
    }

    if (!this.exchanges[exchange].bindings[routingKey]) {
      this.exchanges[exchange].bindings[routingKey] = [];
    }

    if (!this.exchanges[exchange].bindings[routingKey].includes(queue)) {
      this.exchanges[exchange].bindings[routingKey].push(queue);
      logger.info(
        `Queue "${queue}" bound to exchange "${exchange}" with routing key "${routingKey}"`
      );
    }

    return Promise.resolve({});
  },

  publish(exchange, routingKey, content, options = {}) {
    if (exchange && this.exchanges[exchange]) {
      const bindings = this.exchanges[exchange].bindings[routingKey] || [];

      for (const queue of bindings) {
        if (this.queues[queue]) {
          this.queues[queue].push({ content, options });
          logger.info(
            `Message published to queue "${queue}" via exchange "${exchange}" with routing key "${routingKey}"`
          );
        }
      }

      return Promise.resolve(true);
    }

    return Promise.resolve(false);
  },

  sendToQueue(queue, content, options = {}) {
    if (!this.queues[queue]) {
      this.queues[queue] = [];
    }

    this.queues[queue].push({ content, options });
    logger.info(`Message sent directly to queue "${queue}"`);

    return Promise.resolve(true);
  },

  consume(queue, callback) {
    if (!this.queues[queue]) {
      return Promise.reject(new Error(`Queue "${queue}" does not exist`));
    }

    const messages = this.queues[queue];
    const consumerId = `consumer-${Date.now()}-${Math.floor(Math.random() * 1000)}`;

    // Process messages in the queue
    setTimeout(() => {
      if (messages.length > 0) {
        const msg = messages.shift();
        logger.info(`Consuming message from queue "${queue}"`);
        callback({ content: JSON.stringify(msg.content), properties: msg.options });
      }
    }, 100);

    return Promise.resolve({ consumerTag: consumerId });
  },

  // For testing purposes
  getQueueContents(queue) {
    return this.queues[queue] || [];
  },
};

// Create mock Redis client
const mockRedis = {
  keys: {},

  get(key) {
    return Promise.resolve(this.keys[key]);
  },

  set(key, value, options = {}) {
    this.keys[key] = value;
    logger.info(`Redis key "${key}" set to ${value}`);
    return Promise.resolve('OK');
  },

  del(key) {
    delete this.keys[key];
    logger.info(`Redis key "${key}" deleted`);
    return Promise.resolve(1);
  },

  healthCheck() {
    return Promise.resolve({ status: 'ok' });
  },
};

// Simple handler function factory with timeout
const createTimeoutHandler = (queueName, processingTimeMs = 1000) => {
  return async (message, context) => {
    logger.info(`Processing ${queueName} message: ${message.id}`);

    // Simulate processing time with setTimeout
    await new Promise(resolve => setTimeout(resolve, processingTimeMs));

    logger.info(`Completed ${queueName} message: ${message.id}`);
    return { success: true, result: `Processed ${queueName} message` };
  };
};

// Mock worker implementation
class TestWorker {
  constructor() {
    this._handlers = {};
    this._running = false;
    logger.info('Test worker initialized');
  }

  registerStateHandler(currentState, processingState, nextState, handler) {
    const key = `${currentState}:${processingState}:${nextState}`;
    this._handlers[key] = handler;
    logger.info(`Registered handler for state transition: ${key}`);
    return true;
  }

  async start() {
    this._running = true;
    logger.info('Test worker started');
    return true;
  }

  async shutdown() {
    this._running = false;
    logger.info('Test worker stopped');
    return true;
  }

  async processMessage(message) {
    if (!this._running) {
      throw new Error('Worker not started');
    }

    const { state } = message;

    // Find appropriate handler based on current state
    const matchingTransitions = Object.keys(this._handlers).filter(key =>
      key.startsWith(`${state}:`)
    );

    if (matchingTransitions.length === 0) {
      logger.warn(`No handler found for state: ${state}`);
      return null;
    }

    // Use the first matching handler
    const transitionKey = matchingTransitions[0];
    const [currentState, processingState, nextState] = transitionKey.split(':');
    const handler = this._handlers[transitionKey];

    logger.info(
      `Processing message with state transition: ${currentState} -> ${processingState} -> ${nextState}`
    );

    try {
      // Update message state to processing
      message.state = processingState;

      // Process the message
      const result = await handler(message, {});

      // Update message state to next state
      message.state = nextState;

      return { message, result };
    } catch (error) {
      logger.error(`Error processing message: ${error.message}`);
      return { message, error: error.message };
    }
  }
}

// Test publisher implementation
class TestPublisher {
  constructor(rabbitMQClient) {
    this._rabbitMQClient = rabbitMQClient;
    this._queues = ['customer', 'event', 'email', 'users'];
    logger.info('Test publisher initialized');
  }

  async initialize() {
    logger.info('Initializing test publisher...');

    // Connect to RabbitMQ
    await this._rabbitMQClient.connect();

    // Make sure the queues exist
    for (const queue of this._queues) {
      await this._rabbitMQClient.assertQueue(queue);
    }

    logger.info('Test publisher initialized');
  }

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
    await this._rabbitMQClient.sendToQueue(queue, messageWithState);

    return message.id;
  }

  async close() {
    await this._rabbitMQClient.close();
  }
}

// Main test flow
async function runTest() {
  logger.info('Starting test flow...');

  // Initialize test worker
  const worker = new TestWorker();

  // Create handlers for each queue
  const customerHandler = createTimeoutHandler('customer', 500);
  const eventHandler = createTimeoutHandler('event', 300);
  const emailHandler = createTimeoutHandler('email', 400);
  const usersHandler = createTimeoutHandler('users', 200);

  // Start worker
  await worker.start();

  // Register handlers
  worker.registerStateHandler('initial', 'processing', 'completed', customerHandler);
  worker.registerStateHandler('new_event', 'processing_event', 'event_processed', eventHandler);
  worker.registerStateHandler('email_queued', 'sending_email', 'email_sent', emailHandler);
  worker.registerStateHandler('user_action', 'processing_user', 'user_processed', usersHandler);

  // Initialize publisher
  const publisher = new TestPublisher(mockRabbitMQ);
  await publisher.initialize();

  // Publish test messages
  logger.info('Publishing test messages...');
  const messageIds = [];

  for (const queue of ['customer', 'event', 'email', 'users']) {
    const data = {
      test: `Test message for ${queue}`,
      priority: Math.floor(Math.random() * 10),
    };

    const msgId = await publisher.publishMessage(queue, data);
    messageIds.push({ queue, msgId });
  }

  logger.info('Test messages published:', messageIds);

  // Process published messages
  logger.info('Processing messages...');

  for (const queue of ['customer', 'event', 'email', 'users']) {
    const messages = mockRabbitMQ.getQueueContents(queue);

    for (const message of messages) {
      logger.info(`Processing message from queue ${queue}:`, message.content.id);
      await worker.processMessage(message.content);
    }
  }

  // Shutdown
  await worker.shutdown();
  await publisher.close();

  logger.info('Test flow completed successfully');
}

// Run the test
runTest()
  .then(() => {
    logger.info('Test completed successfully');
  })
  .catch(error => {
    logger.error('Test failed:', error);
  });
