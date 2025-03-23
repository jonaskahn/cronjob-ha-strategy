// src/utils/config.js
import fs from 'fs';
import path from 'path';

/**
 * Configuration management for the message processing system.
 * Loads configuration from environment variables and optional config files.
 */
class Config {
  constructor() {
    if (Config.instance) {
      return Config.instance;
    }

    // Default configuration values
    this._config = {
      // Application settings
      app: {
        name: 'message-processor',
        environment: process.env.NODE_ENV || 'development',
        logLevel: process.env.LOG_LEVEL || 'info',
      },

      // RabbitMQ settings
      rabbitmq: {
        clusterHost: process.env.CHJS_RABBITMQ_CLUSTER_HOST || 'admin:admin@localhost:5672',
        managerHost: process.env.CHJS_RABBITMQ_MANAGER_HOST || 'admin:admin@localhost:15672',
        prefetch: parseInt(process.env.CHJS_RABBITMQ_PREFETCH || '10', 10),
        defaultExchangeType: process.env.CHJS_RABBITMQ_DEFAULT_EXCHANGE_TYPE || 'direct',
        heartbeat: parseInt(process.env.CHJS_RABBITMQ_HEARTBEAT || '5000', 10),
      },

      // Redis settings
      redis: {
        clusterHost:
          process.env.CHJS_REDIS_CLUSTER_HOST || 'localhost:6379,localhost:6380,localhost:6381',
        replicaHost: process.env.CHJS_REDIS_CLUSTER_REPLICA_HOST || '',
        keyPrefix: process.env.CHJS_REDIS_KEY_PREFIX || 'MSG:PROCESSING:',
        processingStateTTL: parseInt(process.env.CHJS_REDIS_PROCESSING_TTL || '3600', 10),
      },

      // etcd settings
      etcd: {
        clusterHost:
          process.env.CHJS_ETCD_CLUSTER_HOST || 'localhost:2379,localhost:2380,localhost:2381',
        leaseTTL: parseInt(process.env.CHJS_ETCD_LEASE_TTL || '30', 10),
        heartbeatInterval: parseInt(process.env.CHJS_ETCD_HEARTBEAT_INTERVAL || '10000', 10),
      },

      // Database settings
      database: {
        host: process.env.CHJS_DB_HOST || 'localhost',
        port: parseInt(process.env.CHJS_DB_PORT || '5432', 10),
        user: process.env.CHJS_DB_USER || 'user',
        password: process.env.CHJS_DB_PASSWORD || 'password',
        name: process.env.CHJS_DB_NAME || 'message_processor',
        tableName: process.env.CHJS_DB_TABLE || 'message_states',
        useInMemory: process.env.CHJS_DB_IN_MEMORY === 'true',
      },

      // Consumer settings
      consumer: {
        maxConcurrent: parseInt(process.env.CHJS_CONSUMER_MAX_CONCURRENT || '100', 10),
        verifyFinalState: process.env.CHJS_CONSUMER_VERIFY_FINAL_STATE !== 'false',
        retryDelay: parseInt(process.env.CHJS_CONSUMER_RETRY_DELAY || '1000', 10),
        maxRetries: parseInt(process.env.CHJS_CONSUMER_MAX_RETRIES || '3', 10),
        monitorInterval: parseInt(process.env.CHJS_CONSUMER_MONITOR_INTERVAL || '60000', 10),
      },

      // Queue priority settings
      priority: {
        defaultPriority: parseInt(process.env.CHJS_QUEUE_DEFAULT_PRIORITY || '1', 10),
        highPriority: parseInt(process.env.CHJS_QUEUE_HIGH_PRIORITY || '3', 10),
        mediumPriority: parseInt(process.env.CHJS_QUEUE_MEDIUM_PRIORITY || '2', 10),
        lowPriority: parseInt(process.env.CHJS_QUEUE_LOW_PRIORITY || '1', 10),
        priorityQueuePatterns: {
          high: process.env.CHJS_PRIORITY_HIGH_PATTERNS
            ? process.env.CHJS_PRIORITY_HIGH_PATTERNS.split(',')
            : ['priority-3', 'high', 'urgent'],
          medium: process.env.CHJS_PRIORITY_MEDIUM_PATTERNS
            ? process.env.CHJS_PRIORITY_MEDIUM_PATTERNS.split(',')
            : ['priority-2', 'medium', 'normal'],
          low: process.env.CHJS_PRIORITY_LOW_PATTERNS
            ? process.env.CHJS_PRIORITY_LOW_PATTERNS.split(',')
            : ['priority-1', 'low', 'batch'],
        },
      },
    };

    // Load configuration from file if present
    this._loadConfigFile();

    Config.instance = this;
  }

  /**
   * Get configuration value
   * @param {string} key - Dot-notation configuration key (e.g., 'rabbitmq.clusterHost')
   * @param {any} defaultValue - Default value if key not found
   * @returns {any} - Configuration value
   */
  get(key, defaultValue = null) {
    const parts = key.split('.');
    let value = this._config;

    for (const part of parts) {
      if (value && typeof value === 'object' && part in value) {
        value = value[part];
      } else {
        return defaultValue;
      }
    }

    return value !== undefined ? value : defaultValue;
  }

  /**
   * Get entire configuration object
   * @returns {Object} - Configuration object
   */
  getAll() {
    return { ...this._config };
  }

  /**
   * Set configuration value
   * @param {string} key - Dot-notation configuration key
   * @param {any} value - Value to set
   */
  set(key, value) {
    const parts = key.split('.');
    let target = this._config;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];

      if (!(part in target) || typeof target[part] !== 'object') {
        target[part] = {};
      }

      target = target[part];
    }

    target[parts[parts.length - 1]] = value;
  }

  /**
   * Load configuration from file
   * @private
   */
  _loadConfigFile() {
    const configPath = process.env.CONFIG_PATH || path.join(process.cwd(), 'config');
    const environment = this._config.app.environment;

    // Try environment-specific config first, then fall back to default
    const configFiles = [
      path.join(configPath, `config.${environment}.json`),
      path.join(configPath, 'config.json'),
    ];

    for (const file of configFiles) {
      try {
        if (fs.existsSync(file)) {
          const fileConfig = JSON.parse(fs.readFileSync(file, 'utf8'));
          this._deepMerge(this._config, fileConfig);
          console.log(`Loaded configuration from ${file}`);
          break;
        }
      } catch (error) {
        console.error(`Error loading configuration from ${file}:`, error);
      }
    }
  }

  /**
   * Deep merge objects
   * @param {Object} target - Target object
   * @param {Object} source - Source object
   * @returns {Object} - Merged object
   * @private
   */
  _deepMerge(target, source) {
    for (const key in source) {
      if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
        if (!target[key]) target[key] = {};
        this._deepMerge(target[key], source[key]);
      } else {
        target[key] = source[key];
      }
    }

    return target;
  }
}

// Create singleton instance
const config = new Config();
export default config;
