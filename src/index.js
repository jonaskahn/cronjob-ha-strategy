import getLogger from './utils/logger.js';

// Import client modules
import EtcdClient from './client/etcdClient.js';
import RedisClient from './client/redisClient.js';
import RabbitMQClient from './client/rabbitMQClient.js';

import MessageTracker from './core/messageTracker.js';
import Coordinator from './core/coordinator.js';
import QueueManager from './core/queueManager.js';
import Publisher from './core/publisher.js';
import Consumer from './core/consumer.js';
import { AppConfig } from './utils/appConfig.js';

const logger = getLogger('application');

/**
 * Main application class to manage the message processing system
 */
class Application {
  constructor() {
    this.isInitialized = false;
    this.isShuttingDown = false;
  }

  /**
   * Initialize the application components
   */
  async initialize() {
    try {
      logger.info('Starting application initialization...');

      // Create system configuration
      this.config = new AppConfig();
      logger.info('System configuration loaded');

      // Initialize clients
      await this.initializeClients();

      // Initialize core components
      await this.initializeCoreComponents();

      // Set up signal handlers for graceful shutdown
      this.setupSignalHandlers();

      this.isInitialized = true;
      logger.info('Application initialized successfully');
      return true;
    } catch (error) {
      logger.error('Failed to initialize application:', error);
      await this.shutdown();
      return false;
    }
  }

  /**
   * Initialize client connections
   */
  async initializeClients() {
    logger.info('Initializing clients...');

    // Create client instances
    this.etcdClient = new EtcdClient(this.config);
    this.redisClient = new RedisClient(this.config);
    this.rabbitMQClient = new RabbitMQClient(this.config);

    // Connect to services
    logger.info('Connecting to external services...');
    await this.rabbitMQClient.connect();

    // Check health of connections
    const rabbitHealth = await this.rabbitMQClient.healthCheck();
    const redisHealth = await this.redisClient.healthCheck();
    const etcdHealth = await this.etcdClient.healthCheck();

    if (rabbitHealth.status !== 'ok' || redisHealth.status !== 'ok' || etcdHealth.status !== 'ok') {
      throw new Error('Service health checks failed. Check logs for details.');
    }

    logger.info('All clients connected successfully');
  }

  /**
   * Initialize core components with dependency injection
   */
  async initializeCoreComponents() {
    logger.info('Initializing core components...');

    this.messageTracker = new MessageTracker(this.redisClient, this.config);
    this.stateStorage = new StateStorage(this.config);
    this.coordinator = new Coordinator(this.etcdClient, this.config);

    await this.coordinator.register();

    this.queueManager = new QueueManager(
      this.rabbitMQClient,
      this.coordinator,
      this.messageTracker,
      this.config
    );

    this.publisher = new Publisher(this.rabbitMQClient, this.messageTracker, this.config);

    this.consumer = new Consumer(
      this.queueManager,
      this.messageTracker,
      this.stateStorage,
      this.config
    );

    await this.consumer.initialize();
    await this.queueManager.initialize();

    logger.info('Core components initialized successfully');
  }

  /**
   * Set up signal handlers for graceful shutdown
   */
  setupSignalHandlers() {
    process.on('SIGTERM', async () => {
      logger.info('Received SIGTERM signal');
      await this.shutdown();
      process.exit(0);
    });

    process.on('SIGINT', async () => {
      logger.info('Received SIGINT signal');
      await this.shutdown();
      process.exit(0);
    });

    // Handle uncaught exceptions
    process.on('uncaughtException', async error => {
      logger.error('Uncaught exception:', error);
      await this.shutdown();
      process.exit(1);
    });

    // Handle unhandled promise rejections
    process.on('unhandledRejection', async (reason, promise) => {
      logger.error('Unhandled promise rejection:', reason);
      await this.shutdown();
      process.exit(1);
    });
  }

  /**
   * Start the application
   */
  async start() {
    if (!this.isInitialized) {
      const success = await this.initialize();
      if (!success) {
        return false;
      }
    }

    try {
      logger.info('Starting message consumption...');
      await this.consumer.startConsuming();

      logger.info('Application started successfully');
      return true;
    } catch (error) {
      logger.error('Failed to start application:', error);
      await this.shutdown();
      return false;
    }
  }

  /**
   * Shut down the application gracefully
   */
  async shutdown() {
    if (this.isShuttingDown) {
      logger.info('Shutdown already in progress');
      return;
    }

    this.isShuttingDown = true;
    logger.info('Shutting down application...');

    try {
      // Shutdown in reverse order of initialization
      if (this.consumer) {
        logger.info('Stopping consumer...');
        await this.consumer.shutdown();
      }

      if (this.queueManager) {
        logger.info('Shutting down queue manager...');
        await this.queueManager.shutdown();
      }

      if (this.coordinator) {
        logger.info('Deregistering from coordinator...');
        await this.coordinator.deregister();
      }

      // Close client connections
      if (this.rabbitMQClient) {
        logger.info('Closing RabbitMQ connection...');
        await this.rabbitMQClient.close();
      }

      if (this.redisClient) {
        logger.info('Closing Redis connection...');
        await this.redisClient.quit();
      }

      if (this.etcdClient) {
        logger.info('Closing etcd connection...');
        await this.etcdClient.close();
      }

      logger.info('Application shutdown completed successfully');
    } catch (error) {
      logger.error('Error during application shutdown:', error);
      // Force exit in case of shutdown errors
      process.exit(1);
    }
  }
}

// Create and export application instance
const app = new Application();
export default app;

// Start the application if this is the main entry point
if (import.meta.url === import.meta.main) {
  app.start().catch(error => {
    console.error('Fatal error during application startup:', error);
    process.exit(1);
  });
}
