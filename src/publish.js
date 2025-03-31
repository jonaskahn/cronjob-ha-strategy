import { coordinator, initializeCore, publisher, shutdownCore } from './core/index.js';
import getLogger from './utils/logger.js';
import rabbitMQClient from './client/rabbitMQClient.js';

const logger = getLogger('publish');

// Constants for priority levels
const PRIORITY = {
  HIGH: 3,
  MEDIUM: 2,
  LOW: 1,
};

/**
 * Publish test messages to various exchanges
 */
async function publishTestMessages() {
  try {
    // 1. Publish test order messages - UPDATED exchange name to match consumer registration
    for (let i = 0; i < 3; i++) {
      const orderId = `test-order-${Date.now()}-${i}`;
      const order = {
        items: [
          { productId: `prod-${1000 + i}`, quantity: 2, price: 19.99 },
          { productId: `prod-${2000 + i}`, quantity: 1, price: 29.99 },
        ],
        customerId: `customer-${3000 + i}`,
        paymentMethod: 'credit_card',
      };

      await publisher.publish({
        exchangeName: 'ex.high-priority-orders', // UPDATED to match consumer registration
        routingKey: 'rt.orders',
        message: order,
        messageId: orderId,
        currentState: 'NEW',
        processingState: 'VALIDATING',
        nextState: 'VALIDATED',
        priority: PRIORITY.HIGH,
      });

      logger.info(`Published test order ${orderId}`);
    }

    // 2. Publish test notification messages - UPDATED to match consumer registration
    for (let i = 0; i < 3; i++) {
      const notificationId = `test-notification-${Date.now()}-${i}`;
      const notification = {
        recipient: `test-user-${i}@example.com`,
        message: `Test notification message ${i}`,
        channel: i % 2 === 0 ? 'email' : 'sms',
      };

      await publisher.publish({
        exchangeName: 'ex.notifications', // This already matches registration
        routingKey: 'rt.notifications',
        message: notification,
        messageId: notificationId,
        currentState: 'PENDING',
        processingState: 'SENDING',
        nextState: 'SENT',
        priority: PRIORITY.LOW,
      });

      logger.info(`Published test notification ${notificationId}`);
    }

    // 3. Publish test payment messages - UPDATED to match consumer registration
    for (let i = 0; i < 3; i++) {
      const paymentId = `test-payment-${Date.now()}-${i}`;
      const payment = {
        orderId: `order-${1000 + i}`,
        amount: 49.99 + i * 10,
        paymentMethod: i % 2 === 0 ? 'credit_card' : 'paypal',
        currency: 'USD',
        cardDetails:
          i % 2 === 0
            ? {
                last4: `${4000 + i}`,
                expiryMonth: '12',
                expiryYear: '2025',
              }
            : null,
      };

      await publisher.publish({
        exchangeName: 'ex.payments', // This already matches registration
        routingKey: 'rt.payments',
        message: payment,
        messageId: paymentId,
        currentState: 'PENDING',
        processingState: 'PROCESSING',
        nextState: 'AUTHORIZED',
        priority: PRIORITY.HIGH,
      });

      logger.info(`Published test payment ${paymentId}`);
    }

    // 4. Publish test shipment messages - UPDATED to match consumer registration
    for (let i = 0; i < 3; i++) {
      const shipmentId = `test-shipment-${Date.now()}-${i}`;
      const shipment = {
        orderId: `order-${1000 + i}`,
        items: [
          { productId: `prod-${1000 + i}`, quantity: 2 },
          { productId: `prod-${2000 + i}`, quantity: 1 },
        ],
        shippingAddress: {
          name: `Customer ${i}`,
          street: `${i} Main St`,
          city: 'Example City',
          state: 'EX',
          zip: `10000${i}`,
        },
        serviceLevel: i % 3 === 0 ? 'express' : 'standard',
      };

      await publisher.publish({
        exchangeName: 'ex.shipments', // This already matches registration
        routingKey: 'rt.shipments',
        message: shipment,
        messageId: shipmentId,
        currentState: 'CREATED',
        processingState: 'PICKING',
        nextState: 'PICKED',
        priority: PRIORITY.HIGH,
      });

      logger.info(`Published test shipment ${shipmentId}`);
    }

    // 5. Publish test inventory messages - UPDATED to match consumer registration
    for (let i = 0; i < 3; i++) {
      const inventoryId = `test-inventory-${Date.now()}-${i}`;
      const inventory = {
        productId: `prod-${1000 + i}`,
        quantity: 10 + i * 5,
        location: `warehouse-${(i % 5) + 1}`,
        action: 'allocate',
        orderId: `order-${1000 + i}`,
      };

      await publisher.publish({
        exchangeName: 'ex.inventory', // This already matches registration
        routingKey: 'rt.inventory',
        message: inventory,
        messageId: inventoryId,
        currentState: 'AVAILABLE',
        processingState: 'ALLOCATING',
        nextState: 'ALLOCATED',
        priority: PRIORITY.MEDIUM,
      });

      logger.info(`Published test inventory update ${inventoryId}`);
    }

    // 6. Publish test audit messages - UPDATED to match consumer registration
    for (let i = 0; i < 3; i++) {
      const auditId = `test-audit-${Date.now()}-${i}`;
      const audit = {
        entityType: ['order', 'payment', 'shipment', 'inventory'][i % 4],
        entityId: `entity-${1000 + i}`,
        userId: `user-${i + 1}`,
        action: ['create', 'update', 'delete'][i % 3],
        timestamp: new Date().toISOString(),
        details: { reason: `Test audit event ${i}` },
      };

      await publisher.publish({
        exchangeName: 'ex.audits', // This already matches registration
        routingKey: 'rt.audits',
        message: audit,
        messageId: auditId,
        currentState: 'PENDING',
        processingState: 'PROCESSING',
        nextState: 'COMPLETED',
        priority: PRIORITY.LOW,
      });

      logger.info(`Published test audit event ${auditId}`);
    }

    logger.info('Completed publishing all test messages');
  } catch (error) {
    logger.error('Error publishing test messages:', error);
  }
}

/**
 * Set up the messaging infrastructure (exchanges, queues, bindings)
 * This ensures that all required exchanges and queues exist before publishing
 * @returns {Promise<boolean>} - Success indicator
 */
export async function setupInfrastructure() {
  try {
    logger.info('Setting up messaging infrastructure...');

    // First, declare exchanges
    const exchanges = [
      { name: 'ex.high-priority-orders', type: 'direct' },
      { name: 'ex.notifications', type: 'direct' },
      { name: 'ex.payments', type: 'direct' },
      { name: 'ex.shipments', type: 'direct' },
      { name: 'ex.inventory', type: 'direct' },
      { name: 'ex.audits', type: 'direct' },
    ];

    for (const exchange of exchanges) {
      await rabbitMQClient.assertExchange(exchange.name, exchange.type);
      logger.info(`Declared exchange: ${exchange.name} (${exchange.type})`);
    }

    // Next, declare queues
    const queues = [
      { name: 'q.high-priority-orders', priority: 3 },
      { name: 'q.notifications', priority: 1 },
      { name: 'q.payments', priority: 3 },
      { name: 'q.shipments', priority: 3 },
      { name: 'q.inventory', priority: 2 },
      { name: 'q.audit', priority: 1 },
    ];

    for (const queue of queues) {
      await rabbitMQClient.assertQueue(queue.name);
      logger.info(`Declared queue: ${queue.name} (priority: ${queue.priority})`);
    }

    // Finally, bind queues to exchanges
    const bindings = [
      {
        queue: 'q.high-priority-orders',
        exchange: 'ex.high-priority-orders',
        routingKey: 'rt.orders',
      },
      { queue: 'q.notifications', exchange: 'ex.notifications', routingKey: 'rt.notifications' },
      { queue: 'q.payments', exchange: 'ex.payments', routingKey: 'rt.payments' },
      { queue: 'q.shipments', exchange: 'ex.shipments', routingKey: 'rt.shipments' },
      { queue: 'q.inventory', exchange: 'ex.inventory', routingKey: 'rt.inventory' },
      { queue: 'q.audit', exchange: 'ex.audits', routingKey: 'rt.audits' },
    ];

    for (const binding of bindings) {
      await rabbitMQClient.bindQueue(binding.queue, binding.exchange, binding.routingKey);

      // Also register the binding with the coordinator for priority-based allocation
      await coordinator.setQueuePriority(
        binding.queue,
        queues.find(q => q.name === binding.queue)?.priority || 1,
        {
          exchangeName: binding.exchange,
          routingKey: binding.routingKey,
          exchangeType: exchanges.find(e => e.name === binding.exchange)?.type || 'direct',
        }
      );

      logger.info(
        `Bound queue ${binding.queue} to exchange ${binding.exchange} with routing key ${binding.routingKey}`
      );
    }

    logger.info('Messaging infrastructure setup complete');
    return true;
  } catch (error) {
    logger.error('Error setting up messaging infrastructure:', error);
    return false;
  }
}

/**
 * Main publisher function
 */
async function main() {
  try {
    // Initialize only the components needed for publishing
    await initializeCore({ initConsumer: false });
    logger.info('Core initialized for publisher (publisher-only mode)');

    // Set up all exchanges, queues, and bindings
    await setupInfrastructure();
    logger.info('Messaging infrastructure setup complete');

    // Wait a moment to ensure infrastructure is ready
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Publish test messages
    await publishTestMessages();

    // Exit after publishing
    logger.info('Publisher completed, exiting...');

    // Clean shutdown using publisher-only mode
    await shutdownCore({ shutdownConsumer: false });
    process.exit(0);
  } catch (error) {
    logger.error('Fatal error in publisher:', error);
    process.exit(1);
  }
}

// Start the publisher
main();
