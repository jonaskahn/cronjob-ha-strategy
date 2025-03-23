// src/handlers/index.js
// This file imports and registers all message handlers

// Import core modules needed for handlers
import getLogger from '../utils/logger.js';
import worker from '../worker.js';

const logger = getLogger('handlers');

logger.info('Initializing message handlers');

// Simple handler function factory with timeout
const createTimeoutHandler = (queueName, processingTimeMs = 2000) => {
  return async (message, context) => {
    logger.info(`Processing ${queueName} message: ${message.id}`);

    // Simulate processing time with setTimeout
    await new Promise(resolve => setTimeout(resolve, processingTimeMs));

    logger.info(`Completed ${queueName} message: ${message.id}`);
    return { success: true, result: `Processed ${queueName} message` };
  };
};

// Create handlers for each queue
const customerHandler = createTimeoutHandler('customer', 3000);
const eventHandler = createTimeoutHandler('event', 1500);
const emailHandler = createTimeoutHandler('email', 2500);
const usersHandler = createTimeoutHandler('users', 1000);
const defaultHandler = createTimeoutHandler('default', 2000);

// Initialize handler exports
const handlers = {
  customerHandler,
  eventHandler,
  emailHandler,
  usersHandler,
  defaultHandler,
};

// Function to register handlers after worker is started
export function registerHandlers() {
  logger.info('Registering message handlers with worker');

  // Register handlers for state transitions
  worker.registerStateHandler('initial', 'processing', 'completed', customerHandler);
  worker.registerStateHandler('new_event', 'processing_event', 'event_processed', eventHandler);
  worker.registerStateHandler('email_queued', 'sending_email', 'email_sent', emailHandler);
  worker.registerStateHandler('user_action', 'processing_user', 'user_processed', usersHandler);
  worker.registerStateHandler('default', 'processing_default', 'processed', defaultHandler);

  logger.info('All queue handlers registered');
}

export default handlers;
