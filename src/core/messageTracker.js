import getLogger from '../utils/logger.js';
import redisClient from '../client/redisClient.js';

const logger = getLogger('core/MessageTracker');

/**
 * MessageTracker class for managing message processing states in Redis.
 * Implements consumer-driven state tracking with cleaning after completion.
 */
class MessageTracker {
  /**
   * Create a new MessageTracker instance
   * @param {Object} options - Configuration options
   */
  constructor(options = {}) {
    if (MessageTracker.instance) {
      return MessageTracker.instance;
    }
    this._redisClient = redisClient;
    this._options = {
      keyPrefix: 'MSG:PROCESSING:',
      processingStateTTLInSeconds: 5 * 60,
      localCacheSize: 1000,
      ...options,
    };
    this._localCache = new Map();
    this._localCacheKeys = [];

    MessageTracker.instance = this;
    logger.info('MessageTracker initialized');
  }

  /**
   * Get the Redis key for a message in a specific state
   * @param {string} messageId - Message ID
   * @param {string} processingState - Processing state
   * @returns {string} - Formatted Redis key
   */
  _getKey(messageId, processingState) {
    return `${this._options.keyPrefix}${messageId}:${processingState}`;
  }

  /**
   * Check if a message is currently being processed in a specific state
   * @param {string} messageId - Message ID
   * @param {string} processingState - Processing state
   * @returns {Promise<boolean>} - True if message is being processed
   */
  async isProcessing(messageId, processingState) {
    const key = this._getKey(messageId, processingState);

    // Check local cache first
    const cacheKey = `is-processing:${key}`;
    if (this._localCache.has(cacheKey)) {
      return this._localCache.get(cacheKey);
    }

    try {
      const exists = await this._redisClient.exists(key);
      const isProcessing = exists === 1;

      // Update local cache
      this._updateLocalCache(cacheKey, isProcessing);

      return isProcessing;
    } catch (error) {
      logger.error(`Error checking if message ${messageId} is being processed:`, error);
      // Default to false in case of error
      return false;
    }
  }

  /**
   * Create a processing state for a message
   * @param {string} messageId - Message ID
   * @param {string} processingState - Processing state
   * @param {Object} metadata - Additional metadata
   * @param {number} [ttl] - Optional custom TTL in seconds
   * @returns {Promise<boolean>} - Success indicator
   */
  async createProcessingState(messageId, processingState, metadata = {}, ttl = null) {
    const key = this._getKey(messageId, processingState);
    const stateTTL = ttl || this._options.processingStateTTLInSeconds;

    try {
      // First check if key already exists
      const exists = await this._redisClient.exists(key);
      if (exists === 1) {
        logger.warn(`Message ${messageId} already has processing state ${processingState}`);
        return false;
      }

      // Prepare state data with tracking information
      const stateData = {
        messageId,
        processingState,
        startedAt: Date.now(),
        progress: 0,
        attempts: 1,
        ...metadata,
      };

      // Set in Redis with appropriate TTL
      await this._redisClient.setEx(key, stateTTL, JSON.stringify(stateData));

      // Update local cache
      this._updateLocalCache(`is-processing:${key}`, true);

      logger.debug(
        `Created processing state for message ${messageId} in state ${processingState} with TTL ${stateTTL}s`
      );
      return true;
    } catch (error) {
      logger.error(`Error creating processing state for message ${messageId}:`, error);
      return false;
    }
  }

  /**
   * Delete a processing state after completion or error
   * @param {string} messageId - Message ID
   * @param {string} processingState - Processing state
   * @returns {Promise<boolean>} - Success indicator
   */
  async deleteProcessingState(messageId, processingState) {
    const key = this._getKey(messageId, processingState);

    try {
      const result = await this._redisClient.del(key);

      // Update local cache
      this._updateLocalCache(`is-processing:${key}`, false);

      if (result === 1) {
        logger.debug(
          `Deleted processing state for message ${messageId} in state ${processingState}`
        );
        return true;
      } else {
        logger.warn(
          `Processing state for message ${messageId} in state ${processingState} not found`
        );
        return false;
      }
    } catch (error) {
      logger.error(`Error deleting processing state for message ${messageId}:`, error);
      return false;
    }
  }

  /**
   * Update the progress of a processing state
   * @param {string} messageId - Message ID
   * @param {string} processingState - Processing state
   * @param {number} progress - Progress percentage (0-100)
   * @returns {Promise<boolean>} - Success indicator
   */
  async updateProgress(messageId, processingState, progress) {
    const key = this._getKey(messageId, processingState);

    try {
      // Get current state
      const stateJson = await this._redisClient.get(key);
      if (!stateJson) {
        logger.warn(
          `Unable to update progress: State not found for message ${messageId} in state ${processingState}`
        );
        return false;
      }

      const stateData = JSON.parse(stateJson);

      // Update progress
      stateData.progress = progress;
      stateData.updatedAt = Date.now();

      // Get remaining TTL
      const remainingTTL = await this._redisClient.ttl(key);
      if (remainingTTL < 0) {
        logger.warn(`Key ${key} has no TTL or does not exist`);
        return false;
      }

      // Update in Redis with the remaining TTL
      await this._redisClient.setEx(key, remainingTTL, JSON.stringify(stateData));

      logger.debug(
        `Updated progress to ${progress}% for message ${messageId} in state ${processingState}`
      );
      return true;
    } catch (error) {
      logger.error(`Error updating progress for message ${messageId}:`, error);
      return false;
    }
  }

  /**
   * Refresh TTL for long-running processes
   * @param {string} messageId - Message ID
   * @param {string} processingState - Processing state
   * @param {number} [additionalSeconds] - Additional seconds to add (defaults to original TTL)
   * @returns {Promise<boolean>} - Success indicator
   */
  async refreshTTL(messageId, processingState, additionalSeconds = null) {
    const key = this._getKey(messageId, processingState);
    const ttl = additionalSeconds || this._options.processingStateTTLInSeconds;

    try {
      // Check if key exists
      const exists = await this._redisClient.exists(key);
      if (exists !== 1) {
        logger.warn(
          `Unable to refresh TTL: State not found for message ${messageId} in state ${processingState}`
        );
        return false;
      }

      // Extend TTL
      await this._redisClient.expire(key, ttl);

      logger.debug(`Refreshed TTL for message ${messageId} in state ${processingState}`);
      return true;
    } catch (error) {
      logger.error(`Error refreshing TTL for message ${messageId}:`, error);
      return false;
    }
  }

  /**
   * Get processing state details
   * @param {string} messageId - Message ID
   * @param {string} processingState - Processing state
   * @returns {Promise<Object|null>} - Processing state data or null if not found
   */
  async getProcessingState(messageId, processingState) {
    const key = this._getKey(messageId, processingState);

    try {
      const stateJson = await this._redisClient.get(key);
      if (!stateJson) {
        return null;
      }

      return JSON.parse(stateJson);
    } catch (error) {
      logger.error(`Error getting processing state for message ${messageId}:`, error);
      return null;
    }
  }

  /**
   * Check if multiple messages are being processed
   * @param {Array<Object>} messageInfos - Array of {messageId, processingState} objects
   * @returns {Promise<Object>} - Map of messageId to processing status
   */
  async batchIsProcessing(messageInfos) {
    try {
      // Prepare keys
      const keys = messageInfos.map(info => this._getKey(info.messageId, info.processingState));

      // Get all keys in a single operation
      const results = await this._redisClient.mGet(keys);

      // Map results back to message IDs
      const processingMap = {};
      messageInfos.forEach((info, index) => {
        processingMap[info.messageId] = results[index] !== null;
      });

      return processingMap;
    } catch (error) {
      logger.error('Error batch checking processing status:', error);

      // Return all as not processing in case of error
      const errorMap = {};
      messageInfos.forEach(info => {
        errorMap[info.messageId] = false;
      });
      return errorMap;
    }
  }

  /**
   * Update local cache with size management
   * @param {string} key - Cache key
   * @param {any} value - Value to cache
   */
  _updateLocalCache(key, value) {
    if (this._localCache.has(key)) {
      const index = this._localCacheKeys.indexOf(key);
      if (index !== -1) {
        this._localCacheKeys.splice(index, 1);
      }
    }

    if (this._localCacheKeys.length >= this._options.localCacheSize) {
      const oldestKey = this._localCacheKeys.shift();
      this._localCache.delete(oldestKey);
    }

    this._localCache.set(key, value);
    this._localCacheKeys.push(key);
  }

  /**
   * Clear local cache
   */
  clearCache() {
    this._localCache.clear();
    this._localCacheKeys = [];
    logger.debug('Local cache cleared');
  }
}

// Create singleton instance
const messageTracker = new MessageTracker();
export default messageTracker;
