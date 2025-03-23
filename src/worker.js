// src/worker.js
import getLogger from './utils/logger.js';
import config from './utils/config.js';
import redisClient from './client/redisClient.js';
import rabbitMQClient from './client/rabbitMQClient.js';
import etcdClient from './client/etcdClient.js';
// Commenting out this import if databaseClient doesn't exist
// import databaseClient from './client/databaseClient.js';
import MessageTracker from './core/messageTracker.js';
import StateStorage from './core/storage.js';
import Coordinator from './core/coordinator.js';
import QueueManager from './core/queueManager.js';
import Consumer from './core/consumer.js';

const logger = getLogger('worker');

/**
 * Worker process that consumes messages from RabbitMQ queues
 * based on priority-based assignments. Each worker represents
 * a consumer node in the distributed system.
 */
class Worker {
  constructor() {
    this._config = config;
    this._running = false;
    this._shutdownRequested = false;
    this._shutdownHandled = false;

    // Core components will be initialized in start()
    this._messageTracker = null;
    this._stateStorage = null;
    this._coordinator = null;
    this._queueManager = null;
    this._consumer = null;

    // Set up shutdown handlers
    this._setupShutdownHandlers();

    logger.info('Worker initialized');
  }

  /**
   * Register a state handler with the consumer
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @param {Function} handler - Handler function
   */
  registerStateHandler(currentState, processingState, nextState, handler) {
    if (!this._consumer) {
      throw new Error('Worker not started. Cannot register state handler.');
    }

    this._consumer.registerStateHandler(currentState, processingState, nextState, handler);
  }

  /**
   * Start the worker process
   * @param {Object} options - Startup options
   * @returns {Promise<void>}
   */
  async start(options = {}) {
    if (this._running) {
      logger.warn('Worker already running');
      return;
    }

    logger.info('Starting worker...');

    try {
      // Create component instances if they don't exist already
      this._initializeComponents(options);

      // Connect to services
      await this._connectServices();

      // Initialize components
      await this._initializeStateStorage();
      await this._initializeCoordinator();
      await this._initializeQueueManager();
      await this._initializeConsumer();

      this._running = true;
      logger.info('Worker started successfully');
    } catch (error) {
      logger.error('Failed to start worker:', error);
      await this.shutdown();
      throw error;
    }
  }

  /**
   * Shutdown the worker process
   * @returns {Promise<void>}
   */
  async shutdown() {
    if (!this._running || this._shutdownHandled) {
      return;
    }

    this._shutdownHandled = true;
    this._running = false;
    logger.info('Shutting down worker...');

    try {
      // Shutdown components in reverse order of initialization
      if (this._consumer) {
        await this._consumer.shutdown();
      }

      if (this._queueManager) {
        await this._queueManager.shutdown();
      }

      if (this._coordinator) {
        await this._coordinator.deregister();
      }

      // Close client connections
      try {
        await rabbitMQClient.close();
      } catch (error) {
        logger.error('Error closing RabbitMQ connection:', error);
      }

      try {
        await redisClient.quit();
      } catch (error) {
        logger.error('Error closing Redis connection:', error);
      }

      try {
        await etcdClient.close();
      } catch (error) {
        logger.error('Error closing etcd connection:', error);
      }

      logger.info('Worker shutdown complete');
    } catch (error) {
      logger.error('Error during worker shutdown:', error);
    }
  }

  /**
   * Get worker status and statistics
   * @returns {Object} - Worker status information
   */
  getStatus() {
    return {
      running: this._running,
      shutdownRequested: this._shutdownRequested,
      consumerStats: this._consumer ? this._consumer.getStats() : null,
      activeQueues: this._queueManager ? this._queueManager.getActiveQueues() : [],
      uptime: process.uptime(),
    };
  }

  /**
   * Initialize component instances
   * @param {Object} options - Initialization options
   * @private
   */
  _initializeComponents(options) {
    // Use any provided components or create new ones

    // Initialize message tracker if not provided
    if (!this._messageTracker) {
      this._messageTracker = new MessageTracker(redisClient, {
        keyPrefix: this._config.get('redis.keyPrefix'),
        processingStateTTL: this._config.get('redis.processingStateTTL'),
      });
    }

    // Initialize state storage if not provided
    if (!this._stateStorage) {
      // Using null for database client since it's not imported
      const dbClient = this._config.get('database.useInMemory') ? null : null;
      this._stateStorage = new StateStorage(dbClient, {
        tableName: this._config.get('database.tableName'),
      });
    }

    // Initialize coordinator if not provided
    if (!this._coordinator) {
      this._coordinator = new Coordinator(etcdClient, {
        leaseTTL: this._config.get('etcd.leaseTTL'),
        heartbeatInterval: this._config.get('etcd.heartbeatInterval'),
      });
    }

    // Initialize queue manager if not provided
    if (!this._queueManager) {
      this._queueManager = new QueueManager(
        rabbitMQClient,
        this._coordinator,
        this._messageTracker,
        {
          prefetch: this._config.get('rabbitmq.prefetch'),
        }
      );
    }

    // Initialize consumer if not provided
    if (!this._consumer) {
      this._consumer = new Consumer(this._queueManager, this._messageTracker, this._stateStorage, {
        maxConcurrent: this._config.get('consumer.maxConcurrent'),
        verifyFinalState: this._config.get('consumer.verifyFinalState'),
        retryDelay: this._config.get('consumer.retryDelay'),
        maxRetries: this._config.get('consumer.maxRetries'),
        monitorInterval: this._config.get('consumer.monitorInterval'),
      });
    }
  }

  /**
   * Connect to external services
   * @private
   */
  async _connectServices() {
    // Connect to RabbitMQ
    logger.info('Connecting to RabbitMQ...');
    await rabbitMQClient.connect();

    // Redis and etcd connections are established when the clients are created
    // but we can test the connections here

    // Test Redis connection
    logger.info('Testing Redis connection...');
    const redisHealth = await redisClient.healthCheck();
    if (redisHealth.status !== 'ok') {
      throw new Error(`Redis connection error: ${redisHealth.message}`);
    }

    // Test etcd connection
    logger.info('Testing etcd connection...');
    const etcdHealth = await etcdClient.healthCheck();
    if (etcdHealth.status !== 'ok') {
      throw new Error(`etcd connection error: ${etcdHealth.message}`);
    }

    logger.info('All service connections established');
  }

  /**
   * Initialize state storage
   * @private
   */
  async _initializeStateStorage() {
    logger.info('Initializing state storage...');
    await this._stateStorage.initializeTable();
  }

  /**
   * Initialize coordinator
   * @private
   */
  async _initializeCoordinator() {
    logger.info('Registering with coordinator...');
    await this._coordinator.register();
  }

  /**
   * Initialize queue manager
   * @private
   */
  async _initializeQueueManager() {
    logger.info('Initializing queue manager...');
    await this._queueManager.initialize();
  }

  /**
   * Initialize consumer
   * @private
   */
  async _initializeConsumer() {
    logger.info('Initializing consumer...');
    await this._consumer.initialize();
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

// Create worker instance
const worker = new Worker();

// Export worker
export default worker;

// Start worker if running as main module
if (import.meta.url === import.meta.main) {
  worker.start().catch(error => {
    logger.error('Worker startup failed:', error);
    process.exit(1);
  });
}
