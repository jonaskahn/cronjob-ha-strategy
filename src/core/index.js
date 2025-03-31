// src/core/index.js
import MessageTracker from './messageTracker.js';
import Coordinator from './coordinator.js';
import QueueManager from './queueManager.js';
import Consumer from './consumer.js';
import Publisher from './publisher.js';
import getLogger from '../utils/logger.js';
import { AppConfig } from '../utils/appConfig.js';
import redisClient from '../client/redisClient.js';
import etcdClient from '../client/etcdClient.js';
import rabbitMQClient from '../client/rabbitMQClient.js';

const logger = getLogger('core');

// Create app config
const config = new AppConfig();

// Initialize core components
const messageTracker = new MessageTracker(redisClient, config);
const coordinator = new Coordinator(etcdClient, config);
const queueManager = new QueueManager(rabbitMQClient, coordinator, messageTracker, config);
// Updated: Pass coordinator to Publisher for priority management
const publisher = new Publisher(rabbitMQClient, messageTracker, coordinator, config);
const consumer = new Consumer(queueManager, messageTracker, config);

/**
 * Initialize the core components
 * @returns {Promise<boolean>} - Success indicator
 */
export async function initializeCore() {
  try {
    logger.info('Initializing core components...');
    // Connect to RabbitMQ
    await rabbitMQClient.connect();
    // Initialize coordinator first - handles consumer registration and queue assignments
    await coordinator.initialize();
    // Initialize queue manager - handles RabbitMQ interactions
    await queueManager.initialize();
    // Initialize consumer - processes messages
    await consumer.initialize();
    logger.info('Core components initialized successfully');
    return true;
  } catch (error) {
    logger.error('Failed to initialize core components:', error);
    return false;
  }
}

/**
 * Shut down the core components gracefully
 * @returns {Promise<boolean>} - Success indicator
 */
export async function shutdownCore() {
  try {
    logger.info('Shutting down core components...');
    // Shutdown in reverse order of initialization
    await consumer.shutdown();
    await queueManager.shutdown();
    await coordinator.shutdown();
    logger.info('Core components shut down successfully');
    return true;
  } catch (error) {
    logger.error('Error during core shutdown:', error);
    return false;
  }
}

/**
 * Utility function to declare an exchange and queue
 * @param {string} exchangeName - Exchange name
 * @param {string} queueName - Queue name
 * @param {string} [exchangeType='direct'] - Exchange type
 * @param {Object} [exchangeOptions={}] - Exchange options
 * @param {Object} [queueOptions={}] - Queue options
 * @returns {Promise<boolean>} - Success indicator
 */
const declareExchangeAndQueue = async (
  exchangeName,
  queueName,
  exchangeType = 'direct',
  exchangeOptions = {},
  queueOptions = {}
) => {
  try {
    // Assert exchange
    await rabbitMQClient.assertExchange(exchangeName, exchangeType, exchangeOptions);
    // Assert queue
    await rabbitMQClient.assertQueue(queueName, queueOptions);
    logger.info(`Declared exchange ${exchangeName} and queue ${queueName}`);
    return true;
  } catch (error) {
    logger.error(`Error declaring exchange ${exchangeName} and queue ${queueName}:`, error);
    return false;
  }
};

/**
 * Utility function to bind a queue to an exchange
 * @param {string} queueName - Queue name
 * @param {string} exchangeName - Exchange name
 * @param {string} routingKey - Routing key
 * @param {number} [priority=1] - Queue priority (1=low, 2=medium, 3=high)
 * @param {string} [exchangeType='direct'] - Exchange type
 * @returns {Promise<boolean>} - Success indicator
 */
const bindQueueToExchange = async (
  queueName,
  exchangeName,
  routingKey,
  priority = 1,
  exchangeType = 'direct'
) => {
  try {
    // Store binding information with coordinator
    await coordinator.setQueuePriority(queueName, priority, {
      exchangeName,
      routingKey,
      exchangeType,
    });

    // Perform actual binding
    await queueManager.bindQueueToExchange(queueName, exchangeName, routingKey, exchangeType);

    logger.info(
      `Bound queue ${queueName} to exchange ${exchangeName} with routing key ${routingKey} and priority ${priority}`
    );
    return true;
  } catch (error) {
    logger.error(`Error binding queue ${queueName} to exchange ${exchangeName}:`, error);
    return false;
  }
};

// Export components
export {
  messageTracker,
  coordinator,
  queueManager,
  consumer,
  publisher,
  config,
  declareExchangeAndQueue,
  bindQueueToExchange,
};
