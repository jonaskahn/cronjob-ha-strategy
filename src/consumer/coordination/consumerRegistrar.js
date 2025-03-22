import { v4 as uuidv4 } from 'uuid';
import * as os from 'node:os';
import getLogger from '../../utils/logger.js';
import etcdClient from '../../client/etcdClient.js';

const logger = getLogger('consumer/coordination/ConsumerRegistrar');

class ConsumerRegistrar {
  constructor() {
    this._consumerId = this.#generateConsumerId();
    this._consumerKey = `/rabbitmq-consumer/${this._consumerId}`;
    this._lease = null;
    this._heartbeat = null;
    this._isRegistered = false;
  }

  #generateConsumerId() {
    const hostname = os.hostname();
    const timestamp = Date.now();
    return `CONSUMER-${hostname}-${timestamp}-${uuidv4()}`;
  }

  async register() {
    if (this._isRegistered) return;
    try {
      logger.debug(`Registering consumer ${this._consumerId}`);
      this._lease = await etcdClient.createLease(30);
      const consumerSpecs = {
        id: this._consumerId,
        startTime: Date.now(),
        hostname: os.hostname(),
        capabilities: ['standard'],
        version: '1.0.0',
      };
      await etcdClient.set(this._consumerKey, JSON.stringify(consumerSpecs), this._lease);
      this.startHeartbeat();
      this._isRegistered = true;
    } catch (error) {
      logger.error(`Cannot register consumer ${this._consumerId}`, error);
      throw error;
    }
  }

  startHeartbeat() {
    this._heartbeat = setInterval(async () => {
      try {
        await this._lease.keepAliveOnce();
      } catch (error) {
        logger.error(`Cannot start heart beat`, error);
      }
    }, 10_000);
  }

  async deregister() {
    if (!this._isRegistered) {
      logger.warn('Consumer has not register yet');
      return;
    }
    if (this._heartbeat) {
      clearInterval(this._heartbeat);
    }
    if (this._lease) {
      try {
        await etcdClient.delete(this._consumerKey);
        await this._lease.revoke();
      } catch (error) {
        logger.error(`Failed to deregister: ${this._consumerId}`, error);
      }
    }
  }
}

const consumerRegistrar = new ConsumerRegistrar();

export default consumerRegistrar;
