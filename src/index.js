// src/index.js
import getLogger from './utils/logger.js';
import config from './utils/config.js';
import rabbitMQClient from './client/rabbitMQClient.js';
import etcdClient from './client/etcdClient.js';
import messageTracker from './core/messageTracker.js';
import stateStorage from './core/storage.js';
import coordinator from './core/coordinator.js';
import queueManager from './core/queueManager.js';
import consumer from './core/consumer.js';
import publisher from './core/publisher.js';
import worker from './worker.js';

// Import all handler modules
import { registerHandlers } from './handlers/index.js';

const logger = getLogger('main');

/**
 * Main application class that initializes and manages the entire
 * message processing system.
 */
class Application {
  constructor() {
    this._config = config;
    this._running = false;
    this._shutdownRequested = false;

    // Core components (using singletons)
    this._messageTracker = messageTracker;
    this._stateStorage = stateStorage;
    this._coordinator = coordinator;
    this._queueManager = queueManager;
    this._consumer = consumer;
    this._publisher = publisher;
    this._worker = worker;

    // Set up shutdown handlers
    this._setupShutdownHandlers();

    logger.info('Application initialized');
  }

  /**
   * Start the application
   * @param {Object} options - Startup options
   * @returns {Promise<void>}
   */
  async start(options = {}) {
    if (this._running) {
      logger.warn('Application already running');
      return;
    }

    logger.info('Starting application...');

    try {
      const mode = options.mode || process.env.APP_MODE || 'standalone';

      switch (mode) {
        case 'worker':
          await this._startWorkerMode();
          break;
        case 'coordinator':
          await this._startCoordinatorMode();
          break;
        case 'publisher':
          await this._startPublisherMode();
          break;
        case 'standalone':
        default:
          await this._startStandaloneMode();
          break;
      }

      this._running = true;
      logger.info(`Application started in ${mode} mode`);
    } catch (error) {
      logger.error('Failed to start application:', error);
      await this.shutdown();
      throw error;
    }
  }

  /**
   * Shutdown the application
   * @returns {Promise<void>}
   */
  async shutdown() {
    if (!this._running) {
      return;
    }

    this._running = false;
    logger.info('Shutting down application...');

    try {
      // Shutdown the worker
      await this._worker.shutdown();

      logger.info('Application shutdown complete');
    } catch (error) {
      logger.error('Error during application shutdown:', error);
    }
  }

  /**
   * Start the application in worker mode (consumer only)
   * @private
   */
  async _startWorkerMode() {
    logger.info('Starting in worker mode...');
    await this._worker.start();

    // Register state handlers from handler modules
    this._registerStateHandlers();
  }

  /**
   * Start the application in coordinator mode (coordinator only)
   * @private
   */
  async _startCoordinatorMode() {
    logger.info('Starting in coordinator mode...');

    // Connect to etcd
    const etcdHealth = await etcdClient.healthCheck();
    if (etcdHealth.status !== 'ok') {
      throw new Error(`etcd connection error: ${etcdHealth.message}`);
    }

    // Register with coordinator to get consumer ID
    await this._coordinator.register();

    // Retrieve all queues from RabbitMQ
    await rabbitMQClient.connect();
    const queues = await rabbitMQClient.listQueues();
    const queueNames = queues.map(queue => queue.name);

    // Rebalance consumers across queues
    await this._coordinator.rebalanceConsumers(queueNames);

    // Set up periodic rebalancing
    setInterval(async () => {
      try {
        const currentQueues = await rabbitMQClient.listQueues();
        const currentQueueNames = currentQueues.map(queue => queue.name);
        await this._coordinator.rebalanceConsumers(currentQueueNames);
      } catch (error) {
        logger.error('Error during periodic rebalancing:', error);
      }
    }, 60000); // Rebalance every minute
  }

  /**
   * Start the application in publisher mode (publisher only)
   * @private
   */
  async _startPublisherMode() {
    logger.info('Starting in publisher mode...');

    // Connect to RabbitMQ
    await rabbitMQClient.connect();

    // Initialize state storage
    await this._stateStorage.initializeTable();
  }

  /**
   * Start the application in standalone mode (all components)
   * @private
   */
  async _startStandaloneMode() {
    logger.info('Starting in standalone mode...');

    // Start the worker
    await this._worker.start();

    // Register state handlers from handler modules
    this._registerStateHandlers();
  }

  /**
   * Register all state handlers with the worker
   * @private
   */
  _registerStateHandlers() {
    // Call the exported registerHandlers function
    registerHandlers();
    logger.info('State handlers registered');
  }

  /**
   * Set up graceful shutdown handlers
   * @private
   */
  _setupShutdownHandlers() {
    // Handle process termination signals
    process.on('SIGTERM', this._handleShutdownSignal.bind(this));
    process.on('SIGINT', this._handleShutdownSignal.bind(this));

    // Handle uncaught exceptions
    process.on('uncaughtException', this._handleUncaughtException.bind(this));
    process.on('unhandledRejection', this._handleUnhandledRejection.bind(this));
  }

  /**
   * Handle shutdown signal
   * @param {string} signal - Signal name
   * @private
   */
  _handleShutdownSignal(signal) {
    if (this._shutdownRequested) {
      return;
    }

    this._shutdownRequested = true;
    logger.info(`Received ${signal}. Starting graceful shutdown...`);

    this.shutdown()
      .then(() => {
        logger.info('Graceful shutdown completed');
        process.exit(0);
      })
      .catch(error => {
        logger.error('Error during graceful shutdown:', error);
        process.exit(1);
      });

    // Force exit after timeout
    setTimeout(() => {
      logger.error('Forced shutdown after timeout');
      process.exit(1);
    }, 30000); // 30 seconds
  }

  /**
   * Handle uncaught exception
   * @param {Error} error - Error object
   * @private
   */
  _handleUncaughtException(error) {
    logger.error('Uncaught exception:', error);
    this._handleShutdownSignal('UNCAUGHT_EXCEPTION');
  }

  /**
   * Handle unhandled rejection
   * @param {Error} reason - Rejection reason
   * @param {Promise} promise - Rejected promise
   * @private
   */
  _handleUnhandledRejection(reason, promise) {
    logger.error('Unhandled rejection at:', promise, 'reason:', reason);
  }
}

// Create application instance
const app = new Application();

// Export application
export default app;

// Start application if running as main module
if (import.meta.url === import.meta.main) {
  app.start().catch(error => {
    logger.error('Application startup failed:', error);
    process.exit(1);
  });
}
