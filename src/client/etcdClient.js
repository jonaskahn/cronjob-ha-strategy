import getLogger from '../utils/logger.js';
import { Etcd3 } from 'etcd3';

const logger = getLogger('client/EtcdClient');

/**
 * Client for interacting with etcd for distributed coordination
 */
export default class EtcdClient {
  /**
   * Create a new EtcdClient
   * @param {AppConfig} config - System configuration
   */
  constructor(config) {
    this._config = config.etcd;
    this._client = this._createClient();

    logger.info('EtcdClient initialized with hosts:', this._config.clusterHosts);
  }

  /**
   * Create the etcd client
   * @returns {Etcd3} - etcd client instance
   * @private
   */
  _createClient() {
    return new Etcd3({
      hosts: this._config.clusterHosts,
      retry: {
        retries: this._config.retries,
        initialBackoffMs: this._config.backoff,
      },
    });
  }

  /**
   * Set a key-value pair in etcd
   * @param {string} key - Key to set
   * @param {string} value - Value to set
   * @param {Object} lease - Optional lease object for TTL
   * @returns {Promise<any>} - Result from etcd
   */
  async set(key, value, lease = null) {
    try {
      const putOp = this._client.put(key).value(value);
      if (lease) {
        return await putOp.lease(lease);
      } else {
        return await putOp;
      }
    } catch (error) {
      logger.error(`Error setting key ${key}:`, error);
      throw error;
    }
  }

  /**
   * Get a value from etcd by key
   * @param {string} key - Key to retrieve
   * @returns {Promise<string>} - Value from etcd
   */
  async get(key) {
    try {
      return await this._client.get(key).string();
    } catch (error) {
      logger.error(`Error getting key ${key}:`, error);
      throw error;
    }
  }

  /**
   * Delete a key from etcd
   * @param {string} key - Key to delete
   * @returns {Promise<any>} - Result from etcd
   */
  async delete(key) {
    try {
      return await this._client.delete().key(key);
    } catch (error) {
      logger.error(`Error deleting key ${key}:`, error);
      throw error;
    }
  }

  /**
   * Get all keys with a given prefix
   * @param {string} prefix - Prefix to search for
   * @returns {Promise<Object>} - Object with key-value pairs
   */
  async getWithPrefix(prefix) {
    try {
      return await this._client.getAll().prefix(prefix).strings();
    } catch (error) {
      logger.error(`Error getting keys with prefix ${prefix}:`, error);
      throw error;
    }
  }

  /**
   * Watch for changes on keys with a given prefix
   * @param {string} prefix - Prefix to watch
   * @param {Function} callback - Callback function (event, key, value)
   * @returns {Promise<Object>} - Watcher object
   */
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

  /**
   * Create a lease with specified TTL
   * @param {number} ttlSeconds - TTL in seconds
   * @returns {Promise<Object>} - Lease object
   */
  async createLease(ttlSeconds) {
    try {
      return await this._client.lease(ttlSeconds);
    } catch (error) {
      logger.error(`Error creating lease with TTL ${ttlSeconds}:`, error);
      throw error;
    }
  }

  /**
   * Close the etcd client connection
   * @returns {Promise<void>}
   */
  async close() {
    try {
      await this._client.close();
      logger.info('Etcd client connection closed');
    } catch (error) {
      logger.error('Error closing etcd client:', error);
      throw error;
    }
  }

  /**
   * Check etcd connection health
   * @returns {Promise<Object>} - Health status object
   */
  async healthCheck() {
    try {
      const testKey = 'health-check-' + Date.now();
      await this.set(testKey, 'OK');
      const value = await this.get(testKey);
      await this.delete(testKey);

      if (value === 'OK') {
        return { status: 'ok', message: 'Etcd connection is healthy' };
      } else {
        return { status: 'error', message: 'Etcd health check failed' };
      }
    } catch (error) {
      logger.error('Etcd health check failed:', error);
      return { status: 'error', message: error.message };
    }
  }
}
