import getLogger from '../utils/logger.js';
import Redis from 'ioredis';
import appConfig from '../utils/appConfig.js';

const logger = getLogger('client/RedisClient');

/**
 * Client for interacting with Redis for temporary state storage
 */
export class RedisClient {
  /**
   * Create a new RedisClient
   * @param {AppConfig} config - System configuration
   */
  constructor(config) {
    this._config = config.redis;
    this._client = this._createClusterClient();
    this._setupEventHandlers();
  }

  /**
   * Create a Redis cluster client
   * @returns {Object} Redis cluster client
   * @private
   */
  _createClusterClient() {
    const { masterNodes, replicaNodes } = this._getClusterNodes();
    const startupNodes = [...masterNodes, ...replicaNodes];

    logger.debug(
      `Connecting to Redis Cluster using ${startupNodes.length} nodes: ${JSON.stringify(startupNodes)}`
    );

    return new Redis.Cluster(startupNodes, this._getClusterOptions());
  }

  /**
   * Get cluster nodes from configuration
   * @returns {Object} Object containing master and replica nodes
   * @private
   */
  _getClusterNodes() {
    const masterNodes = this._config.clusterHosts.map(host => {
      const [hostname, port] = host.split(':');
      return { host: hostname, port: Number(port) };
    });

    const replicaNodes = this._config.replicaHosts.map(host => {
      const [hostname, port] = host.split(':');
      return { host: hostname, port: Number(port) };
    });

    logger.info(`Using master nodes: ${JSON.stringify(masterNodes)}`);
    logger.info(`Using replica nodes: ${JSON.stringify(replicaNodes)}`);

    return { masterNodes, replicaNodes };
  }

  /**
   * Get cluster options for Redis
   * @returns {Object} Cluster options
   * @private
   */
  _getClusterOptions() {
    return {
      redisOptions: {
        enableAutoPipelining: true,
        showFriendlyErrorStack: true,
        connectTimeout: 10000,
        commandTimeout: 10000,
        reconnectOnError: err => {
          logger.warn(`Redis reconnection triggered due to: ${err.message}`);
          return true;
        },
      },
      scaleReads: 'all',
      maxRedirections: 64,
      retryDelayOnFailover: 100,
      retryDelayOnClusterDown: 500,
      clusterRetryStrategy: times => {
        logger.debug(`Redis cluster retry attempt ${times}`);
        return Math.min(100 * times, 3000);
      },
      enableReadyCheck: true,
      enableOfflineQueue: true,
      clusterRetryRandomDelay: 200,
      refreshAfterSet: true,
      slotsRefreshTimeout: 5000,
      slotsRefreshInterval: 15000,
      natMap: this._config.natMap,
    };
  }

  /**
   * Set up event handlers for Redis client
   * @private
   */
  _setupEventHandlers() {
    this._client.on('error', err => {
      logger.error('Redis Cluster Error:', err);
    });

    this._client.on('connect', () => {
      logger.info('Connected to Redis Cluster');
    });

    this._client.on('reconnecting', () => {
      logger.warn('Reconnecting to Redis Cluster');
    });

    this._client.on('ready', async () => {
      logger.info('Redis Cluster is ready to use');
      await this._verifyClusterConnection();
    });

    this._client.on('end', () => {
      logger.warn('Redis Cluster connection ended');
    });

    this._client.on('node error', (err, node) => {
      if (node && node.options) {
        logger.error(`Redis Cluster Node ${node.options.host}:${node.options.port} Error:`, err);
      } else {
        logger.error(`Redis Cluster Node Error:`, err);
      }
    });
  }

  /**
   * Verify Redis cluster connection
   * @returns {Promise<void>}
   * @private
   */
  async _verifyClusterConnection() {
    try {
      // Set a health check key
      await this._client.set('cluster:health', 'OK');
      const value = await this._client.get('cluster:health');
      if (value === 'OK') {
        logger.info('Redis Cluster health check passed');
      } else {
        logger.warn(`Redis Cluster health check returned unexpected value: ${value}`);
      }
    } catch (error) {
      logger.error(`Redis Cluster health check failed: ${error.message}`);
    }
  }

  /**
   * Set a key-value pair in Redis
   * @param {string} key - Key to set
   * @param {string} value - Value to set
   * @param {string} [flag] - Optional flag (EX, PX, NX, XX)
   * @param {number} [time] - Optional time value for EX/PX flags
   * @returns {Promise<string>} - Redis response
   */
  async set(key, value, flag, time) {
    logger.debug(`Setting key: ${key}`);
    try {
      if (flag && time) {
        return await this._client.set(key, value, flag, time);
      } else {
        return await this._client.set(key, value);
      }
    } catch (error) {
      logger.error(`Failed to set key ${key}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get a value from Redis by key
   * @param {string} key - Key to retrieve
   * @returns {Promise<string>} - Value from Redis
   */
  async get(key) {
    try {
      return await this._client.get(key);
    } catch (error) {
      logger.error(`Failed to get key ${key}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Delete a key from Redis
   * @param {string} key - Key to delete
   * @returns {Promise<number>} - Number of keys deleted
   */
  async del(key) {
    try {
      return await this._client.del(key);
    } catch (error) {
      logger.error(`Failed to delete key ${key}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Check if a key exists in Redis
   * @param {string} key - Key to check
   * @returns {Promise<number>} - 1 if exists, 0 if not
   */
  async exists(key) {
    try {
      return await this._client.exists(key);
    } catch (error) {
      logger.error(`Failed to check if key ${key} exists: ${error.message}`);
      throw error;
    }
  }

  /**
   * Set key value with expiration
   * @param {string} key - Key to set
   * @param {number} seconds - Expiration time in seconds
   * @param {string} value - Value to set
   * @returns {Promise<string>} - Redis response
   */
  async setEx(key, seconds, value) {
    try {
      return await this._client.setex(key, seconds, value);
    } catch (error) {
      logger.error(`Failed to set key ${key} with expiration: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get multiple keys at once
   * @param {Array<string>} keys - Array of keys to get
   * @returns {Promise<Array>} - Array of values
   */
  async mGet(keys) {
    try {
      return await this._client.mget(keys);
    } catch (error) {
      logger.error(`Failed to get multiple keys: ${error.message}`);
      throw error;
    }
  }

  /**
   * Execute multiple operations in a pipeline
   * @param {Function} operations - Function that receives the pipeline and adds operations
   * @returns {Promise<Array>} - Results of all operations
   */
  async pipeline(operations) {
    try {
      const pipeline = this._client.pipeline();
      operations(pipeline);
      return await pipeline.exec();
    } catch (error) {
      logger.error(`Failed to execute pipeline: ${error.message}`);
      throw error;
    }
  }

  /**
   * Close the Redis connection
   * @returns {Promise<void>}
   */
  async quit() {
    try {
      await this._client.quit();
      logger.info('Disconnected from Redis Cluster');
    } catch (error) {
      logger.error(`Error disconnecting from Redis Cluster: ${error.message}`);
      throw error;
    }
  }

  /**
   * Check Redis connection health
   * @returns {Promise<Object>} - Health status object
   */
  async healthCheck() {
    try {
      const testKey = 'health-check-' + new Date().getTime();
      await this.set(testKey, 'OK');
      const value = await this.get(testKey);
      await this.del(testKey);

      if (value === 'OK') {
        return { status: 'ok', message: 'Redis connection is healthy' };
      } else {
        return { status: 'error', message: 'Redis health check failed' };
      }
    } catch (error) {
      logger.error('Redis health check failed:', error);
      return { status: 'error', message: error.message };
    }
  }
}

const redisClient = new RedisClient(appConfig);
export default redisClient;
