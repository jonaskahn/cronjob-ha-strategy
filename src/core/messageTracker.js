import getLogger from '../utils/logger.js';

const logger = getLogger('core/MessageTracker');

/**
 * MessageTracker manages temporary message processing states in Redis
 */
export default class MessageTracker {
  /**
   * Create a new MessageTracker
   * @param {RedisClient} redisClient - Redis client instance
   * @param {AppConfig} config - System configuration
   */
  constructor(redisClient, config) {
    this._redisClient = redisClient;
    this._keyPrefix = config.messageTracker.keyPrefix;
    this._processingStateTTL = config.messageTracker.processingStateTTL;
    this._localCache = new Map();
    this._localCacheKeys = [];
    this._localCacheMaxSize = config.messageTracker.localCacheMaxSize;

    logger.info('MessageTracker initialized');
  }

  /**
   * Get the Redis key for a message in a specific state
   * @param {string} messageId - Message ID
   * @param {string} processingState - Processing state
   * @returns {string} - Formatted Redis key
   */
  _getKey(messageId, processingState) {
    return `${this._keyPrefix}${messageId}:${processingState}`;
  }

  /**
   * Check if a message is currently being processed in a specific state
   * @param {string} messageId - Message ID
   * @param {string} processingState - Processing state
   * @returns {Promise<boolean>} - True if message is being processed
   */
  async isProcessing(messageId, processingState) {
    const key = this._getKey(messageId, processingState);

    const cacheKey = `is-processing:${key}`;
    if (this._localCache.has(cacheKey)) {
      return this._localCache.get(cacheKey);
    }

    try {
      const exists = await this._redisClient.exists(key);
      const isProcessing = exists === 1;
      this._updateLocalCache(cacheKey, isProcessing);
      return isProcessing;
    } catch (error) {
      logger.error(`Error checking if message ${messageId} is being processed:`, error);
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
    const stateTTL = ttl || this._processingStateTTL;

    try {
      const exists = await this._redisClient.exists(key);
      if (exists === 1) {
        logger.warn(`Message ${messageId} already has processing state ${processingState}`);
        return false;
      }

      const stateData = {
        messageId,
        processingState,
        startedAt: Date.now(),
        progress: 0,
        attempts: 1,
        ...metadata,
      };

      await this._redisClient.setEx(key, stateTTL, JSON.stringify(stateData));

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
   * Check if multiple messages are being processed
   * @param {Array<Object>} messageInfos - Array of {messageId, processingState} objects
   * @returns {Promise<Object>} - Map of messageId to processing status
   */
  async batchIsProcessing(messageInfos) {
    try {
      const keys = messageInfos.map(info => this._getKey(info.messageId, info.processingState));
      const results = await this._redisClient.mGet(keys);
      const processingMap = {};
      messageInfos.forEach((info, index) => {
        processingMap[info.messageId] = results[index] !== null;
      });

      return processingMap;
    } catch (error) {
      logger.error('Error batch checking processing status:', error);
      const errorMap = {};
      messageInfos.forEach(info => {
        errorMap[info.messageId] = false;
      });
      return errorMap;
    }
  }

  /**
   * Create multiple processing state entries in a batch
   * @param {Array<Object>} stateEntries - Array of state entry objects
   * @returns {Promise<Object>} - Map of messageId to success status
   */
  async batchCreateProcessingStates(stateEntries) {
    const results = {};

    try {
      await this._redisClient.pipeline(pipeline => {
        for (const entry of stateEntries) {
          const key = this._getKey(entry.messageId, entry.processingState);
          const stateData = {
            messageId: entry.messageId,
            processingState: entry.processingState,
            startedAt: Date.now(),
            progress: 0,
            attempts: 1,
            ...entry.metadata,
          };

          pipeline.setex(key, this._processingStateTTL, JSON.stringify(stateData));
        }
      });

      stateEntries.forEach(entry => {
        results[entry.messageId] = true;
        const key = this._getKey(entry.messageId, entry.processingState);
        this._updateLocalCache(`is-processing:${key}`, true);
      });

      logger.debug(`Batch created ${stateEntries.length} processing states`);
      return results;
    } catch (error) {
      logger.error('Error batch creating processing states:', error);

      stateEntries.forEach(entry => {
        results[entry.messageId] = false;
      });

      return results;
    }
  }

  /**
   * Update local cache with size management
   * @param {string} key - Cache key
   * @param {any} value - Value to cache
   * @private
   */
  _updateLocalCache(key, value) {
    if (this._localCache.has(key)) {
      const index = this._localCacheKeys.indexOf(key);
      if (index !== -1) {
        this._localCacheKeys.splice(index, 1);
      }
    }

    if (this._localCacheKeys.length >= this._localCacheMaxSize) {
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
