export class AppConfig {
  constructor(options = {}) {
    this.etcd = {
      clusterHosts: this.parseEnv(options?.etcd?.hosts, process.env.CHJS_ETCD_CLUSTER_HOST, [
        'localhost:2379',
        'localhost:2380',
        'localhost:2381',
      ]),
      retries: Number(options?.etcd?.retries || process.env.CHJS_ETCD_CLUSTER_RETRIES_TIMES || 3),
      backoff: Number(
        options?.etcd?.backoff || process.env.CHJS_ETCD_CLUSTER_RETRIES_BACKOFF || 1000
      ),
    };

    this.redis = {
      clusterHosts: this.parseEnv(options?.redis?.hosts, process.env.CHJS_REDIS_CLUSTER_HOST, [
        'localhost:6382',
        'localhost:6383',
        'localhost:6384',
      ]),
      replicaHosts: this.parseEnv(
        options?.redis?.replicas,
        process.env.CHJS_REDIS_CLUSTER_REPLICA_HOST,
        ['localhost:6379', 'localhost:6380', 'localhost:6381']
      ),
      natMap: {
        '172.18.0.2:6379': { host: 'localhost', port: 6379 },
        '172.18.0.3:6379': { host: 'localhost', port: 6380 },
        '172.18.0.4:6379': { host: 'localhost', port: 6381 },
        '172.18.0.5:6379': { host: 'localhost', port: 6382 },
        '172.18.0.6:6379': { host: 'localhost', port: 6383 },
        '172.18.0.7:6379': { host: 'localhost', port: 6384 },
      },
    };

    this.rabbitmq = {
      connectionUrl:
        options?.rabbitmq?.url ||
        process.env.CHJS_RABBITMQ_CLUSTER_HOST ||
        'admin:admin@localhost:5672',
      heartbeat: Number(
        options?.rabbitmq?.heartbeat || process.env.CHJS_RABBITMQ_CLUSTER_HEARTBEAT || 5000
      ),
      managerHost:
        options?.rabbitmq?.managerHost ||
        process.env.CHJS_RABBITMQ_MANAGER_HOST ||
        'admin:admin@localhost:15672',
      prefetch: Number(options?.rabbitmq?.prefetch || process.env.CHJS_RABBITMQ_PREFETCH || 10),
      defaultExchangeType:
        options?.rabbitmq?.exchangeType ||
        process.env.CHJS_RABBITMQ_DEFAULT_EXCHANGE_TYPE ||
        'topic',
      defaultExchangeOptions: {
        durable: true,
        autoDelete: false,
      },
      defaultQueueOptions: {
        durable: true,
        autoDelete: false,
      },
    };

    this.coordinator = {
      consumerPrefix: options?.coordinator?.consumerPrefix || '/rabbitmq-consumers/',
      queueAssignmentPrefix: options?.coordinator?.queueAssignmentPrefix || '/queue-assignments/',
      queueConfigPrefix: options?.coordinator?.queueConfigPrefix || '/queue-configs/',
      leaseTTLInSeconds: options?.coordinator?.leaseTTL || 30,
      heartbeatIntervalInMs: options?.coordinator?.heartbeatInterval || 10_000,
    };

    this.messageTracker = {
      keyPrefix: options?.messageTracker?.keyPrefix || 'MSG:PROCESSING:',
      processingStateTTL: options?.messageTracker?.stateTTL || 300,
      localCacheMaxSize: options?.messageTracker?.cacheSize || 1000,
    };

    this.consumer = {
      maxConcurrent: options?.consumer?.maxConcurrent || 100,
      verifyFinalState: options?.consumer?.verifyFinalState !== false,
      retryDelay: options?.consumer?.retryDelay || 1000,
      maxRetries: options?.consumer?.maxRetries || 3,
      monitorInterval: options?.consumer?.monitorInterval || 60000,
    };

    this.publisher = {
      defaultExchangeType: options?.publisher?.defaultExchangeType || 'direct',
      confirmPublish: options?.publisher?.confirmPublish !== false,
      defaultHeaders: options?.publisher?.defaultHeaders || {},
    };
  }

  parseEnv(optionValue, envValue, defaultValue) {
    if (optionValue) {
      return Array.isArray(optionValue) ? optionValue : [optionValue];
    }
    if (envValue) {
      return envValue.split(',');
    }
    return defaultValue;
  }
}
