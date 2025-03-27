import cron from 'node-cron';
import { v4 as uuidv4 } from 'uuid';
import getLogger from '../utils/logger.js';
import { consumer, publisher, queueManager } from '../core/index.js';

const logger = getLogger('jobs/AbstractCronJob');

/**
 * AbstractCronJob provides a base class for jobs that can both publish and consume messages
 * Either role can be disabled via environment variables
 */
export default class AbstractCronJob {
  /**
   * Create a new AbstractCronJob
   * @param {Object} options - Job configuration options
   * @param {string} options.name - Job name for identification
   * @param {string} options.schedule - Cron schedule expression
   * @param {boolean} [options.enablePublisher=true] - Whether to enable publisher role
   * @param {boolean} [options.enableConsumer=true] - Whether to enable consumer role
   * @param {AppConfig} [options.config] - System configuration
   */
  constructor(options) {
    this._name = options.name;
    this._schedule = options.schedule;

    this._enablePublisher = this._getEnvFlag('ENABLE_PUBLISHER', options.enablePublisher !== false);
    this._enableConsumer = this._getEnvFlag('ENABLE_CONSUMER', options.enableConsumer !== false);

    this._config = options.config || {};
    this._publisher = publisher;
    this._consumer = consumer;
    this._queueManager = queueManager;

    // Track state and statistics
    this._job = null;
    this._lastRun = null;
    this._successCount = 0;
    this._errorCount = 0;
    this._isRunning = false;

    this._initialize();

    logger.info(
      `AbstractCronJob "${this._name}" initialized with publisher=${this._enablePublisher}, consumer=${this._enableConsumer}`
    );
  }

  /**
   * Get flag from environment variable
   * @param {string} name - Environment variable name suffix
   * @param {boolean} defaultValue - Default value if not set
   * @returns {boolean} - Flag value
   * @private
   */
  _getEnvFlag(name, defaultValue) {
    const envName = `CHJS_JOB_${this._name.toUpperCase()}_${name}`;
    const envValue = process.env[envName];

    if (envValue === undefined) {
      return defaultValue;
    }

    return envValue === '1' || envValue.toLowerCase() === 'true';
  }

  /**
   * Initialize the cron job
   * @private
   */
  _initialize() {
    // Create cron job
    this._job = cron.schedule(
      this._schedule,
      async () => {
        try {
          await this._execute();
        } catch (error) {
          logger.error(`Error executing job "${this._name}":`, error);
          this._errorCount++;
        }
      },
      {
        scheduled: false, // Don't start immediately
      }
    );

    // Initialize consumer if enabled
    if (this._enableConsumer) {
      this._initializeConsumer();
    }
  }

  /**
   * Initialize consumer capabilities
   * @private
   */
  async _initializeConsumer() {
    try {
      // Register handlers
      const handlers = this.getMessageHandlers();

      if (!handlers || handlers.length === 0) {
        logger.warn(`Job "${this._name}" has consumer enabled but no handlers defined`);
        return;
      }

      for (const handler of handlers) {
        const { currentState, processingState, nextState, callback } = handler;

        if (!currentState || !processingState || !nextState || typeof callback !== 'function') {
          logger.warn(`Invalid handler defined in job "${this._name}"`);
          continue;
        }

        // Register handler with consumer
        this._consumer.registerStateHandler(
          currentState,
          processingState,
          nextState,
          async (message, metadata) => {
            try {
              await callback.call(this, message, metadata);
              return true;
            } catch (error) {
              logger.error(
                `Error in handler for ${currentState}:${processingState}:${nextState}:`,
                error
              );
              throw error;
            }
          }
        );

        logger.info(
          `Registered handler for ${currentState}:${processingState}:${nextState} in job "${this._name}"`
        );
      }
    } catch (error) {
      logger.error(`Error initializing consumer for job "${this._name}":`, error);
    }
  }

  /**
   * Execute the job - calls publishMessages and/or processMessages based on configuration
   * @private
   */
  async _execute() {
    this._lastRun = new Date();
    this._isRunning = true;

    try {
      let success = true;

      // Execute publisher if enabled
      if (this._enablePublisher) {
        const publishSuccess = await this._executePublisher();
        success = success && publishSuccess;
      }

      // Execute any custom logic
      const customSuccess = await this.executeCustomLogic();
      success = success && customSuccess;

      if (success) {
        this._successCount++;
      } else {
        this._errorCount++;
      }

      return success;
    } catch (error) {
      this._errorCount++;
      logger.error(`Error in job "${this._name}":`, error);
      return false;
    } finally {
      this._isRunning = false;
    }
  }

  /**
   * Execute publisher logic
   * @returns {Promise<boolean>} - Success indicator
   * @private
   */
  async _executePublisher() {
    try {
      const messagesToPublish = await this.generateMessages();

      if (!messagesToPublish || messagesToPublish.length === 0) {
        logger.debug(`No messages to publish from job "${this._name}"`);
        return true;
      }

      let allSuccess = true;

      for (const msgConfig of messagesToPublish) {
        const {
          queueName,
          exchangeName,
          routingKey = queueName,
          currentState,
          processingState,
          nextState,
          content,
          priority = 1,
        } = msgConfig;

        // Skip invalid configurations
        if (!this._validateMessageConfig(msgConfig)) {
          allSuccess = false;
          continue;
        }

        // Generate message ID if not provided
        const messageId = msgConfig.messageId || `${this._name}-${uuidv4()}`;

        // Set queue priority if specified
        if (priority > 1 && queueName) {
          await this._publisher._coordinator.setQueuePriority(queueName, priority);
        }

        // Publish message
        let result;
        if (exchangeName) {
          // Publish to exchange
          result = await this._publisher.publish({
            exchangeName,
            routingKey,
            message: content,
            messageId,
            currentState,
            processingState,
            nextState,
          });
        } else {
          // Publish directly to queue
          result = await this._publisher.publishToQueue({
            queueName,
            message: content,
            messageId,
            currentState,
            processingState,
            nextState,
          });
        }

        if (!result) {
          allSuccess = false;
          logger.warn(
            `Failed to publish message to ${exchangeName || queueName} from job "${this._name}"`
          );
        }
      }

      return allSuccess;
    } catch (error) {
      logger.error(`Error in publisher for job "${this._name}":`, error);
      return false;
    }
  }

  /**
   * Validate message configuration
   * @param {Object} config - Message configuration
   * @returns {boolean} - Whether configuration is valid
   * @private
   */
  _validateMessageConfig(config) {
    // Must have either queueName or exchangeName
    if (!config.queueName && !config.exchangeName) {
      logger.warn(`Message missing queueName or exchangeName in job "${this._name}"`);
      return false;
    }

    // Must have state transition information
    if (!config.currentState || !config.processingState || !config.nextState) {
      logger.warn(`Message missing state transition info in job "${this._name}"`);
      return false;
    }

    // Must have content
    if (!config.content) {
      logger.warn(`Message missing content in job "${this._name}"`);
      return false;
    }

    return true;
  }

  /**
   * Start the cron job
   */
  start() {
    this._job.start();
    logger.info(`Started job "${this._name}" with schedule "${this._schedule}"`);
  }

  /**
   * Stop the cron job
   */
  stop() {
    this._job.stop();
    logger.info(`Stopped job "${this._name}"`);
  }

  /**
   * Run the job manually (without waiting for schedule)
   * @returns {Promise<boolean>} - Success indicator
   */
  async runNow() {
    logger.info(`Manually executing job "${this._name}"`);
    return await this._execute();
  }

  /**
   * Get job statistics
   * @returns {Object} - Statistics object
   */
  getStats() {
    return {
      name: this._name,
      schedule: this._schedule,
      lastRun: this._lastRun,
      successCount: this._successCount,
      errorCount: this._errorCount,
      isRunning: this._isRunning,
      publisher: {
        enabled: this._enablePublisher,
      },
      consumer: {
        enabled: this._enableConsumer,
      },
    };
  }

  /**
   * Shutdown the job
   */
  shutdown() {
    this.stop();
    logger.info(`Shutdown job "${this._name}"`);
  }

  // ------------------- ABSTRACT METHODS (TO BE IMPLEMENTED BY SUBCLASSES) -------------------

  /**
   * Generate messages to be published
   * Must be implemented by subclasses if publisher is enabled
   * @returns {Promise<Array<Object>>} - Array of message configurations
   */
  async generateMessages() {
    if (this._enablePublisher) {
      logger.warn(
        `Job "${this._name}" has publisher enabled but generateMessages() not implemented`
      );
    }
    return [];
  }

  /**
   * Get message handlers for consumer role
   * Must be implemented by subclasses if consumer is enabled
   * @returns {Array<Object>} - Array of handler configurations
   */
  getMessageHandlers() {
    if (this._enableConsumer) {
      logger.warn(
        `Job "${this._name}" has consumer enabled but getMessageHandlers() not implemented`
      );
    }
    return [];
  }

  /**
   * Execute custom job logic (beyond publishing/consuming)
   * Optional method for subclasses to implement
   * @returns {Promise<boolean>} - Success indicator
   */
  async executeCustomLogic() {
    return true;
  }
}
