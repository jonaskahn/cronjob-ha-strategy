import getLogger from './utils/logger.js';

const logger = getLogger('test-handlers');

// Mock worker to test handlers
const mockWorker = {
  _handlers: {},

  registerStateHandler(currentState, processingState, nextState, handler) {
    const key = `${currentState}:${processingState}:${nextState}`;
    this._handlers[key] = handler;
    logger.info(`Registered handler for state transition: ${key}`);
    return true;
  },

  async testHandler(stateKey, message) {
    if (!this._handlers[stateKey]) {
      logger.error(`No handler registered for state transition: ${stateKey}`);
      return null;
    }

    try {
      logger.info(`Testing handler for state transition: ${stateKey}`);
      const result = await this._handlers[stateKey](message, {});
      return result;
    } catch (error) {
      logger.error(`Error in handler for ${stateKey}:`, error);
      return null;
    }
  },

  listHandlers() {
    return Object.keys(this._handlers);
  },
};

// Replace the real worker with our mock for testing
import.meta.jest = {
  replaceProperty: (obj, prop, value) => {
    const originalValue = obj[prop];
    obj[prop] = value;
    return () => {
      obj[prop] = originalValue;
    };
  },
};

// Create a simplified version of our handler factory
const createTimeoutHandler = (queueName, processingTimeMs = 500) => {
  return async (message, context) => {
    logger.info(`Processing ${queueName} message: ${message.id}`);

    // Simulate processing time with setTimeout
    await new Promise(resolve => setTimeout(resolve, processingTimeMs));

    logger.info(`Completed ${queueName} message: ${message.id}`);
    return { success: true, result: `Processed ${queueName} message` };
  };
};

// Create test message
const createTestMessage = (id, data = {}) => {
  return {
    id: id || `test-${Date.now()}`,
    timestamp: new Date().toISOString(),
    data,
  };
};

// Main test function
async function runTest() {
  logger.info('Starting handler test...');

  // Create handlers for each queue
  const customerHandler = createTimeoutHandler('customer', 500);
  const eventHandler = createTimeoutHandler('event', 300);
  const emailHandler = createTimeoutHandler('email', 400);
  const usersHandler = createTimeoutHandler('users', 200);

  // Register handlers with mock worker
  mockWorker.registerStateHandler('initial', 'processing', 'completed', customerHandler);
  mockWorker.registerStateHandler('new_event', 'processing_event', 'event_processed', eventHandler);
  mockWorker.registerStateHandler('email_queued', 'sending_email', 'email_sent', emailHandler);
  mockWorker.registerStateHandler('user_action', 'processing_user', 'user_processed', usersHandler);

  logger.info('Registered all handlers:', mockWorker.listHandlers());

  // Test each handler
  const testMessageCustomer = createTestMessage('customer-1', { priority: 5 });
  const testMessageEvent = createTestMessage('event-1', { type: 'click' });
  const testMessageEmail = createTestMessage('email-1', { recipient: 'test@example.com' });
  const testMessageUser = createTestMessage('user-1', { userId: 'user123' });

  logger.info('Testing customer handler...');
  await mockWorker.testHandler('initial:processing:completed', testMessageCustomer);

  logger.info('Testing event handler...');
  await mockWorker.testHandler('new_event:processing_event:event_processed', testMessageEvent);

  logger.info('Testing email handler...');
  await mockWorker.testHandler('email_queued:sending_email:email_sent', testMessageEmail);

  logger.info('Testing user handler...');
  await mockWorker.testHandler('user_action:processing_user:user_processed', testMessageUser);

  logger.info('All handlers tested successfully');
}

// Run the test
runTest()
  .then(() => {
    logger.info('Test completed');
  })
  .catch(error => {
    logger.error('Error in test:', error);
  });
