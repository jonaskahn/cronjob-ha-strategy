import getLogger from '../utils/logger.js';
import Redis from 'ioredis';

const logger = getLogger('client/RedisClient');

class RedisClient {
  constructor() {
    this._client = this.#createClusterClient();
    this.#setupEventHandlers();
  }

  #createClusterClient() {
    const { masterNodes, replicaNodes } = this.#getClusterNodes();
    const startupNodes = [...masterNodes, ...replicaNodes];

    logger.debug(
      `Connecting to Redis Cluster using ${startupNodes.length} nodes: ${JSON.stringify(startupNodes)}`
    );

    return new Redis.Cluster(startupNodes, this.#getClusterOptions());
  }

  #getClusterNodes() {
    let masterNodes = [];
    let replicaNodes = [];

    const redisClusterHost = process.env.CHJS_REDIS_CLUSTER_HOST;
    if (redisClusterHost) {
      masterNodes = redisClusterHost.split(',').map(x => {
        const [host, port] = x.split(':');
        return { host, port: Number(port) };
      });
      logger.info(
        `Using master nodes from env.CHJS_REDIS_CLUSTER_HOST: ${JSON.stringify(masterNodes)}`
      );
    } else {
      logger.info(`Fallback to default master nodes`);
      masterNodes = [
        { host: 'localhost', port: 6382 },
        { host: 'localhost', port: 6383 },
        { host: 'localhost', port: 6384 },
      ];
    }

    const redisClusterReplicaHost = process.env.CHJS_REDIS_CLUSTER_REPLICA_HOST;
    if (redisClusterReplicaHost) {
      replicaNodes = redisClusterReplicaHost.split(',').map(x => {
        const [host, port] = x.split(':');
        return { host, port: Number(port) };
      });
      logger.info(
        `Using replica nodes from env.CHJS_REDIS_CLUSTER_REPLICA_HOST: ${JSON.stringify(replicaNodes)}`
      );
    } else {
      logger.info(`Fallback to default replica nodes`);
      replicaNodes = [
        { host: 'localhost', port: 6379 },
        { host: 'localhost', port: 6380 },
        { host: 'localhost', port: 6381 },
      ];
    }

    return { masterNodes, replicaNodes };
  }

  #getClusterOptions() {
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
      maxRedirections: 32,
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
      natMap: this.#getNatMapConfig(),
    };
  }

  #getNatMapConfig() {
    return {
      '172.18.0.2:6379': { host: 'localhost', port: 6379 },
      '172.18.0.3:6379': { host: 'localhost', port: 6380 },
      '172.18.0.4:6379': { host: 'localhost', port: 6381 },
      '172.18.0.5:6379': { host: 'localhost', port: 6382 },
      '172.18.0.6:6379': { host: 'localhost', port: 6383 },
      '172.18.0.7:6379': { host: 'localhost', port: 6384 },
    };
  }

  #setupEventHandlers() {
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
      await this.#verifyClusterConnection();
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

  async #verifyClusterConnection() {
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
   * Delete multiple keys from Redis in one operation
   * @param {Array<string>} keys - Keys to delete
   * @returns {Promise<number>} - Number of keys deleted
   */
  async multiDel(keys) {
    try {
      return await this._client.del(...keys);
    } catch (error) {
      logger.error(`Failed to delete multiple keys: ${error.message}`);
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
   * Check multiple keys for existence in a single operation
   * @param {Array<string>} keys - Array of keys to check
   * @returns {Promise<Object>} - Map of key to existence boolean
   */
  async multiExists(keys) {
    if (!keys || keys.length === 0) {
      return {};
    }

    try {
      // Use pipeline for better performance
      const pipeline = this._client.pipeline();
      for (const key of keys) {
        pipeline.exists(key);
      }

      const results = await pipeline.exec();
      const existenceMap = {};

      keys.forEach((key, index) => {
        // results format is [err, result] for each operation
        existenceMap[key] = results[index][1] === 1;
      });

      return existenceMap;
    } catch (error) {
      logger.error(`Failed to check multiple keys existence: ${error.message}`);
      throw error;
    }
  }

  /**
   * Set key value with expiration
   * @param {string} key - Key to set
   * @param {string} value - Value to set
   * @param {number} seconds - Expiration time in seconds
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
   * Set multiple keys with the same expiration in one operation
   * @param {Object} keyValues - Object with key-value pairs
   * @param {number} seconds - Expiration time in seconds for all keys
   * @returns {Promise<string>} - Redis response
   */
  async multiSetEx(keyValues, seconds) {
    try {
      // Prepare arguments for the script
      const args = [];
      for (const [key, value] of Object.entries(keyValues)) {
        args.push(key, JSON.stringify(value), seconds.toString());
      }

      // Execute the multi-set script
      return await this._client.multiSetEx(args);
    } catch (error) {
      logger.error(`Failed to set multiple keys with expiration: ${error.message}`);
      throw error;
    }
  }

  /**
   * Set expiration on a key
   * @param {string} key - Key to set expiration on
   * @param {number} seconds - Expiration time in seconds
   * @returns {Promise<number>} - 1 if set, 0 if key doesn't exist
   */
  async expire(key, seconds) {
    try {
      return await this._client.expire(key, seconds);
    } catch (error) {
      logger.error(`Failed to set expiration for key ${key}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Check and set a key only if it doesn't exist
   * @param {string} key - Key to set
   * @param {string} value - Value to set
   * @returns {Promise<number>} - 1 if set, 0 if not set
   */
  async setNx(key, value) {
    try {
      return await this._client.setnx(key, value);
    } catch (error) {
      logger.error(`Failed to set key ${key} if not exists: ${error.message}`);
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
   * Execute a Lua script
   * @param {string} script - Lua script to execute
   * @param {number} numKeys - Number of keys
   * @param {Array} args - Script arguments
   * @returns {Promise<any>} - Script result
   */
  async eval(script, numKeys, ...args) {
    try {
      return await this._client.eval(script, numKeys, ...args);
    } catch (error) {
      logger.error(`Failed to execute Lua script: ${error.message}`);
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
      const testKey = 'health-check-' + Date.now();
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

const redisClient = new RedisClient();
export default redisClient;
