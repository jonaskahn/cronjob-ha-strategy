import EtcdClient from './etcdClient.js';
import { AppConfig } from '../utils/appConfig.js';
import RedisClient from './redisClient.js';
import RabbitMQClient from './rabbitMQClient.js';
import getLogger from '../utils/logger.js';

const logger = getLogger('client');

const appConfig = new AppConfig();
const etcdClient = new EtcdClient(appConfig);
const redisClient = new RedisClient(appConfig);
const rabbitMQClient = new RabbitMQClient(appConfig);
(async () => {
  try {
    await rabbitMQClient.connect();
  } catch (error) {
    logger.warn('Initial RabbitMQ connection failed, will retry when needed:', error.message);
  }
})();

export { etcdClient, redisClient, rabbitMQClient };
