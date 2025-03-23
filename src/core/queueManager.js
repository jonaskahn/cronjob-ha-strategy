import getLogger from '../utils/logger.js';
import rabbitMQClient from '../client/rabbitMQClient.js';
import coordinator from './coordinator.js';
import messageTracker from './messageTracker.js';

const logger = getLogger('core/QueueManager');

/**
 * QueueManager handles RabbitMQ queue operations and integrates with
 * Coordinator for dynamic consumer-queue assignment based on priorities.
 */
class QueueManager {
  /**
   * Create a new QueueManager instance
   * @param {Object} options - Configuration options
   */
  constructor(options = {}) {
    if (QueueManager.instance) {
      return QueueManager.instance;
    }
    this._rabbitMQClient = rabbitMQClient;
    this._coordinator = coordinator;
    this._messageTracker = messageTracker;
    this._options = {
      prefetch: 10,
      defaultQueueOptions: {
        durable: true,
        autoDelete: false,
      },
      defaultExchangeOptions: {
        durable: true,
        autoDelete: false,
      },
      stateHandlers: new Map(),
      ...options,
    };

    this._activeConsumers = new Map();
    this._declaredQueues = new Set();
    this._queueConfigs = new Map();
    this._coordinator.onAssignmentChanged = this.#handleAssignmentChange.bind(this);

    QueueManager.instance = this;
    logger.info('QueueManager initialized');
  }

  /**
   * Initialize the QueueManager and start processing assigned queues
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      // Register with coordinator
      await this._coordinator.register();

      // Get initial queue assignments
      const assignedQueues = await this._coordinator.getAssignedQueues();

      // Start consuming from assigned queues
      if (assignedQueues.length > 0) {
        logger.info(`Starting consumption for ${assignedQueues.length} assigned queues`);
        for (const queueName of assignedQueues) {
          await this.startConsumingQueue(queueName);
        }
      } else {
        logger.info('No queues assigned initially');
      }

      logger.info('QueueManager initialized successfully');
    } catch (error) {
      logger.error('Error initializing QueueManager:', error);
      throw error;
    }
  }

  /**
   * Register a handler for a specific state transition
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next (final) state
   * @param {Function} handler - Handler function (message, metadata) => Promise<void>
   */
  registerStateHandler(currentState, processingState, nextState, handler) {
    const stateKey = this.#getStateTransitionKey(currentState, processingState, nextState);
    this._options.stateHandlers.set(stateKey, handler);

    logger.info(
      `Registered handler for state transition: ${currentState} -> ${processingState} -> ${nextState}`
    );
  }

  /**
   * Create and bind a queue to an exchange
   * @param {string} queueName - Queue name
   * @param {string} exchangeName - Exchange name
   * @param {string} pattern - Binding pattern/routing key
   * @param {string} [exchangeType='direct'] - Exchange type
   * @param {Object} [queueOptions={}] - Queue options
   * @param {Object} [exchangeOptions={}] - Exchange options
   * @returns {Promise<Object>} - Queue information
   */
  async createQueue(
    queueName,
    exchangeName,
    pattern,
    exchangeType = 'direct',
    queueOptions = {},
    exchangeOptions = {}
  ) {
    try {
      // Assert exchange
      await this._rabbitMQClient.assertExchange(exchangeName, exchangeType, {
        ...this._options.defaultExchangeOptions,
        ...exchangeOptions,
      });

      // Assert queue
      const queueInfo = await this._rabbitMQClient.assertQueue(queueName, {
        ...this._options.defaultQueueOptions,
        ...queueOptions,
      });

      // Bind queue to exchange
      await this._rabbitMQClient.bindQueue(queueName, exchangeName, pattern);

      // Track queue
      this._declaredQueues.add(queueName);
      this._queueConfigs.set(queueName, {
        exchangeName,
        exchangeType,
        pattern,
        queueOptions,
        exchangeOptions,
      });

      logger.info(
        `Created and bound queue ${queueName} to ${exchangeName} with pattern ${pattern}`
      );
      return queueInfo;
    } catch (error) {
      logger.error(`Error creating queue ${queueName}:`, error);
      throw error;
    }
  }

  /**
   * Start consuming messages from a queue
   * @param {string} queueName - Queue name
   * @returns {Promise<string>} - Consumer tag
   */
  async startConsumingQueue(queueName) {
    try {
      // Check if already consuming
      if (this._activeConsumers.has(queueName)) {
        logger.debug(`Already consuming from queue ${queueName}`);
        return this._activeConsumers.get(queueName);
      }

      // Check if queue is assigned to this consumer
      const isAssigned = await this._coordinator.isQueueAssigned(queueName);
      if (!isAssigned) {
        logger.warn(`Queue ${queueName} is not assigned to this consumer, skipping`);
        return null;
      }

      // Ensure queue exists
      if (!this._declaredQueues.has(queueName)) {
        await this._rabbitMQClient.assertQueue(queueName, this._options.defaultQueueOptions);
        this._declaredQueues.add(queueName);
      }

      // Start consuming
      const consumerTag = await this._rabbitMQClient.consume(
        queueName,
        message => this.#processMessage(message, queueName),
        { prefetch: this._options.prefetch }
      );

      // Track active consumer
      this._activeConsumers.set(queueName, consumerTag);

      logger.info(`Started consuming from queue ${queueName} with tag ${consumerTag}`);
      return consumerTag;
    } catch (error) {
      logger.error(`Error starting consumption from queue ${queueName}:`, error);
      throw error;
    }
  }

  /**
   * Stop consuming messages from a queue
   * @param {string} queueName - Queue name
   * @returns {Promise<boolean>} - Success indicator
   */
  async stopConsumingQueue(queueName) {
    try {
      // Check if consuming
      if (!this._activeConsumers.has(queueName)) {
        logger.debug(`Not consuming from queue ${queueName}, nothing to stop`);
        return true;
      }

      // Cancel consumer
      await this._rabbitMQClient.cancelConsumer(queueName);

      // Remove from active consumers
      this._activeConsumers.delete(queueName);

      logger.info(`Stopped consuming from queue ${queueName}`);
      return true;
    } catch (error) {
      logger.error(`Error stopping consumption from queue ${queueName}:`, error);
      return false;
    }
  }

  /**
   * Delete a queue
   * @param {string} queueName - Queue name
   * @returns {Promise<boolean>} - Success indicator
   */
  async deleteQueue(queueName) {
    try {
      // Stop consuming first
      if (this._activeConsumers.has(queueName)) {
        await this.stopConsumingQueue(queueName);
      }

      // Delete queue
      await this._rabbitMQClient.deleteQueue(queueName);

      // Remove tracking
      this._declaredQueues.delete(queueName);
      this._queueConfigs.delete(queueName);

      logger.info(`Deleted queue ${queueName}`);
      return true;
    } catch (error) {
      logger.error(`Error deleting queue ${queueName}:`, error);
      return false;
    }
  }

  /**
   * Get all active queues being consumed
   * @returns {Array<string>} - Array of queue names
   */
  getActiveQueues() {
    return Array.from(this._activeConsumers.keys());
  }

  /**
   * Get all declared queues
   * @returns {Array<string>} - Array of queue names
   */
  getDeclaredQueues() {
    return Array.from(this._declaredQueues);
  }

  /**
   * Shutdown QueueManager and clean up resources
   * @returns {Promise<void>}
   */
  async shutdown() {
    try {
      // Stop all consumers
      const activeQueues = this.getActiveQueues();
      for (const queueName of activeQueues) {
        await this.stopConsumingQueue(queueName);
      }

      // Deregister from coordinator
      await this._coordinator.deregister();

      logger.info('QueueManager shutdown complete');
    } catch (error) {
      logger.error('Error during QueueManager shutdown:', error);
      throw error;
    }
  }

  /**
   * Handle queue assignment changes from coordinator
   * @param {Array<string>} newQueues - New queue assignments
   * @private
   */
  async #handleAssignmentChange(newQueues) {
    try {
      // Get current active queues
      const currentQueues = this.getActiveQueues();

      // Find queues to stop consuming
      const queuesToStop = currentQueues.filter(q => !newQueues.includes(q));

      // Find queues to start consuming
      const queuesToStart = newQueues.filter(q => !currentQueues.includes(q));

      // Stop consuming from removed queues
      for (const queueName of queuesToStop) {
        await this.stopConsumingQueue(queueName);
      }

      // Start consuming from new queues
      for (const queueName of queuesToStart) {
        await this.startConsumingQueue(queueName);
      }

      logger.info(
        `Queue assignments updated: Stopped ${queuesToStop.length}, Started ${queuesToStart.length}`
      );
    } catch (error) {
      logger.error('Error handling assignment change:', error);
    }
  }

  /**
   * Process a message received from RabbitMQ
   * @param {Object} message - RabbitMQ message
   * @param {string} queueName - Queue name
   * @private
   */
  async #processMessage(message, queueName) {
    // Extract message ID and state information
    const headers = message.properties.headers || {};
    const messageId = headers['x-message-id'] || message.properties.messageId;
    const currentState = headers['x-current-state'];
    const processingState = headers['x-processing-state'];
    const nextState = headers['x-next-state'];

    if (!messageId) {
      logger.warn(`Received message without ID from queue ${queueName}, rejecting`);
      await message.reject(false);
      return;
    }

    if (!currentState || !processingState || !nextState) {
      logger.warn(
        `Received message ${messageId} without state information from queue ${queueName}, rejecting`
      );
      await message.reject(false);
      return;
    }

    try {
      // 1. Check if already processing in Redis
      const isProcessing = await this._messageTracker.isProcessing(messageId, currentState);
      if (isProcessing) {
        logger.warn(
          `Message ${messageId} already being processed in state ${currentState}, skipping`
        );
        await message.ack(); // Acknowledge to remove from queue
        return;
      }

      // 2. Create processing state in Redis
      await this._messageTracker.createProcessingState(messageId, currentState, {
        processingState,
        nextState,
        queueName,
        startedAt: Date.now(),
      });

      // 3. Find and execute handler
      const stateKey = this.#getStateTransitionKey(currentState, processingState, nextState);
      if (this._options.stateHandlers.has(stateKey)) {
        const handler = this._options.stateHandlers.get(stateKey);

        // Parse message content
        let content;
        try {
          content = message.json();
        } catch (error) {
          content = message.content.toString();
        }

        // Process message
        await handler(content, {
          messageId,
          currentState,
          processingState,
          nextState,
          headers,
          queueName,
        });

        // 4. Delete processing state from Redis
        await this._messageTracker.deleteProcessingState(messageId, currentState);

        // 5. Acknowledge message
        await message.ack();

        logger.debug(
          `Successfully processed message ${messageId} with state transition: ${currentState} -> ${processingState} -> ${nextState}`
        );
      } else {
        logger.warn(
          `No handler for state transition: ${currentState} -> ${processingState} -> ${nextState}`
        );

        // Delete processing state since we're not handling it
        await this._messageTracker.deleteProcessingState(messageId, currentState);

        await message.reject(false); // Don't requeue
      }
    } catch (error) {
      logger.error(`Error processing message ${messageId} from queue ${queueName}:`, error);

      // Delete processing state on error
      await this._messageTracker.deleteProcessingState(messageId, currentState);

      // Reject message
      await message.reject(false); // Don't requeue
    }
  }

  /**
   * Get a unique key for state transition
   * @param {string} currentState - Current state
   * @param {string} processingState - Processing state
   * @param {string} nextState - Next state
   * @returns {string} - State transition key
   * @private
   */
  #getStateTransitionKey(currentState, processingState, nextState) {
    return `${currentState}:${processingState}:${nextState}`;
  }
}

const queueManager = new QueueManager();
export default queueManager;
