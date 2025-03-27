import { etcdClient, rabbitMQClient, redisClient } from '../client';
import MessageTracker from './messageTracker.js';
import Coordinator from './coordinator.js';
import QueueManager from './queueManager.js';
import Consumer from './consumer.js';
import Publisher from './publisher.js';
import getLogger from '../utils/logger.js';
import { AppConfig } from '../utils/appConfig.js';

const logger = getLogger('core');

const config = new AppConfig();

const messageTracker = new MessageTracker(redisClient, config);
const coordinator = new Coordinator(etcdClient, config);
const queueManager = new QueueManager(rabbitMQClient, coordinator, messageTracker, config);
const publisher = new Publisher(rabbitMQClient, messageTracker, config);
const consumer = new Consumer(queueManager, messageTracker, config);
export { messageTracker, coordinator, queueManager, consumer, publisher, config };

export async function initializeCore() {
  try {
    logger.info('Initializing core components...');

    // Initialize components that require async setup
    await coordinator.register();
    await queueManager.initialize();
    await consumer.initialize();

    logger.info('Core components initialized successfully');
    return true;
  } catch (error) {
    logger.error('Failed to initialize core components:', error);
    return false;
  }
}

// Function to shut down the core system
export async function shutdownCore() {
  try {
    logger.info('Shutting down core components...');

    // Shut down in reverse order of dependencies
    await consumer.shutdown();
    await queueManager.shutdown();
    await coordinator.deregister();

    logger.info('Core components shut down successfully');
    return true;
  } catch (error) {
    logger.error('Error during core shutdown:', error);
    return false;
  }
}
