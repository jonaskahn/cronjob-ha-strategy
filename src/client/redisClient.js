import getLoggerInstance from '../utils/logger.js';
import Redis from 'ioredis';

const logger = getLoggerInstance('client/RedisClient');

class RedisClient {
  constructor() {
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

    const startupNodes = [...masterNodes, ...replicaNodes];
    logger.debug(
      `Connecting to Redis Cluster using ${startupNodes.length} nodes: ${JSON.stringify(startupNodes)}`
    );

    this._client = new Redis.Cluster(startupNodes, {
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
      natMap: {
        '172.18.0.2:6379': { host: 'localhost', port: 6379 },
        '172.18.0.3:6379': { host: 'localhost', port: 6380 },
        '172.18.0.4:6379': { host: 'localhost', port: 6381 },
        '172.18.0.5:6379': { host: 'localhost', port: 6382 },
        '172.18.0.6:6379': { host: 'localhost', port: 6383 },
        '172.18.0.7:6379': { host: 'localhost', port: 6384 },
      },
    });

    this.#postConstruct();
  }

  #postConstruct() {
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

  // Verify that the cluster connection is working by testing a simple operation
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

  async set(key, value) {
    logger.debug(`Start to set key: ${key}, value: *****`);
    try {
      await this._client.set(key, value);
      logger.debug('Finish to set key: %s', key);
    } catch (error) {
      logger.error(`Failed to set key ${key}: ${error.message}`);
      throw error;
    }
  }

  async get(key) {
    try {
      return await this._client.get(key);
    } catch (error) {
      logger.error(`Failed to get key ${key}: ${error.message}`);
      throw error;
    }
  }

  // Helper methods for more Redis operations
  async del(key) {
    try {
      return await this._client.del(key);
    } catch (error) {
      logger.error(`Failed to delete key ${key}: ${error.message}`);
      throw error;
    }
  }

  async exists(key) {
    try {
      return await this._client.exists(key);
    } catch (error) {
      logger.error(`Failed to check if key ${key} exists: ${error.message}`);
      throw error;
    }
  }

  // Disconnect from Redis Cluster
  async disconnect() {
    try {
      await this._client.quit();
      logger.info('Disconnected from Redis Cluster');
    } catch (error) {
      logger.error(`Error disconnecting from Redis Cluster: ${error.message}`);
    }
  }
}

const redisClient = new RedisClient();

export default redisClient;
