import getLogger from '../utils/logger.js';
import { Etcd3 } from 'etcd3';

const logger = getLogger('client/EtcdClient');

class EtcdClient {
  constructor() {
    const clusterHosts = process.env.CHJS_REDIS_CLUSTER_HOST
      ? process.env.CHJS_REDIS_CLUSTER_HOST.split(',')
      : ['localhost:2379', 'localhost:2380', 'localhost:2381'];
    this._client = new Etcd3({
      hosts: clusterHosts,
      retry: {
        retries: 3,
        initialBackoffMs: 300,
      },
    });
  }

  async set(key, value, lease) {
    try {
      return await this._client.put(key).value(value).lease(lease);
    } catch (error) {
      logger.error(`Error setting key ${key}:`, error);
      throw error;
    }
  }

  async get(key) {
    try {
      return await this._client.get(key).string();
    } catch (error) {
      logger.error(`Error getting key ${key}:`, error);
      throw error;
    }
  }

  async delete(key) {
    try {
      return await this._client.delete().key(key);
    } catch (error) {
      logger.error(`Error deleting key ${key}:`, error);
      throw error;
    }
  }

  async getWithPrefix(prefix) {
    try {
      return await this._client.getAll().prefix(prefix).strings();
    } catch (error) {
      logger.error(`Error getting keys with prefix ${prefix}:`, error);
      throw error;
    }
  }

  async watchPrefix(prefix, callback) {
    try {
      return this._client
        .watch()
        .prefix(prefix)
        .create()
        .then(watcher => {
          watcher.on('put', event => {
            callback('put', event.key.toString(), event.value.toString());
          });
          watcher.on('delete', event => {
            callback('delete', event.key.toString());
          });
          return watcher;
        });
    } catch (error) {
      logger.error(`Error watching prefix ${prefix}:`, error);
      throw error;
    }
  }

  async createLease(ttlSeconds) {
    try {
      return await this._client.lease(ttlSeconds);
    } catch (error) {
      logger.error(`Error creating lease with TTL ${ttlSeconds}:`, error);
      throw error;
    }
  }

  async close() {
    try {
      await this._client.close();
    } catch (error) {
      logger.error('Error closing etcd client:', error);
      throw error;
    }
  }
}

const etcdClient = new EtcdClient();

export default etcdClient;
