// src/utils/rabbitMQUtils.js
import getLogger from './logger.js';

const logger = getLogger('utils/RabbitMQUtils');

export class RabbitMQUtils {
  constructor(rabbitMQClient, options = {}) {
    this._rabbitMQClient = rabbitMQClient;
    this._options = {
      defaultExchangeType: options.defaultExchangeType || 'direct',
      defaultQueueOptions: options.defaultQueueOptions || { durable: true },
      defaultExchangeOptions: options.defaultExchangeOptions || { durable: true },
    };

    // Shared caches
    this._declaredExchanges = new Map();
    this._declaredQueues = new Set();
  }

  /**
   * Safely ensure an exchange exists, handling type mismatches
   * @param {string} exchangeName - Exchange name
   * @param {string} exchangeType - Desired exchange type
   * @returns {Promise<string>} - The actual exchange type used
   */
  async safeEnsureExchangeExists(exchangeName, exchangeType) {
    if (!exchangeName) return exchangeType || this._options.defaultExchangeType;

    // If we already know this exchange, use the known type
    if (this._declaredExchanges.has(exchangeName)) {
      const existingType = this._declaredExchanges.get(exchangeName);
      if (existingType !== exchangeType) {
        logger.warn(
          `Using existing exchange type '${existingType}' for exchange '${exchangeName}' instead of requested '${exchangeType}'`
        );
      }
      return existingType;
    }

    try {
      // Try to create with requested type
      const typeToUse = exchangeType || this._options.defaultExchangeType;
      await this._rabbitMQClient.assertExchange(exchangeName, typeToUse);
      this._declaredExchanges.set(exchangeName, typeToUse);
      return typeToUse;
    } catch (error) {
      // Check if error is due to type mismatch
      if (
        error.message &&
        error.message.includes('PRECONDITION_FAILED') &&
        error.message.includes("inequivalent arg 'type'")
      ) {
        // Extract the actual type from error message
        const match = error.message.match(/received '(\w+)' but current is '(\w+)'/);
        if (match && match[2]) {
          const actualType = match[2];
          logger.warn(
            `Exchange '${exchangeName}' exists with type '${actualType}'. Adapting to use this type.`
          );

          // Try again with the actual type
          try {
            await this._rabbitMQClient.assertExchange(exchangeName, actualType);
            this._declaredExchanges.set(exchangeName, actualType);
            return actualType;
          } catch (retryError) {
            logger.error(`Failed to assert exchange with actual type:`, retryError);
            throw retryError;
          }
        }
      }

      logger.error(`Error ensuring exchange ${exchangeName} exists:`, error);
      throw error;
    }
  }

  /**
   * Safely assert a queue exists with retry mechanism
   * @param {string} queueName - Queue name
   * @param {Object} options - Queue options
   * @returns {Promise<void>}
   */
  async safeAssertQueue(queueName, options = {}) {
    const queueOptions = {
      ...this._options.defaultQueueOptions,
      ...options,
    };

    try {
      await this._rabbitMQClient.assertQueue(queueName, queueOptions);
      this._declaredQueues.add(queueName);
    } catch (error) {
      logger.error(`Error asserting queue ${queueName}:`, error);

      // If we get a connection error, wait and retry once
      if (
        error.message &&
        (error.message.includes('Connection closed') || error.message.includes('Channel closed'))
      ) {
        logger.info(`Retrying queue assertion for ${queueName} after connection issue`);
        await new Promise(resolve => setTimeout(resolve, 1000));
        await this._rabbitMQClient.assertQueue(queueName, queueOptions);
        this._declaredQueues.add(queueName);
      } else {
        throw error;
      }
    }
  }

  /**
   * Check if a queue has been declared
   * @param {string} queueName - Queue name
   * @returns {boolean} - Whether queue has been declared
   */
  isQueueDeclared(queueName) {
    return this._declaredQueues.has(queueName);
  }

  /**
   * Get the type of a declared exchange
   * @param {string} exchangeName - Exchange name
   * @returns {string|null} - Exchange type or null if not declared
   */
  getDeclaredExchangeType(exchangeName) {
    return this._declaredExchanges.get(exchangeName) || null;
  }
}
