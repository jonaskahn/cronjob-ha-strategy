import { declareExchangeAndQueue, initializeCore, publisher } from './core/index.js';
import getLogger from './utils/logger.js';

const logger = getLogger('publish');

// Constants for priority levels
const PRIORITY = {
  HIGH: 3,
  MEDIUM: 2,
  LOW: 1,
};

/**
 * Setup RabbitMQ infrastructure for publishing
 */
async function setupPublisherInfrastructure() {
  try {
    // Define our exchanges
    const exchanges = [
      { name: 'orders', type: 'direct' },
      { name: 'payments', type: 'direct' },
      { name: 'notifications', type: 'direct' },
      { name: 'shipments', type: 'direct' },
      { name: 'inventory', type: 'direct' },
      { name: 'audits', type: 'direct' },
    ];

    // Create exchanges
    logger.info('Setting up RabbitMQ exchanges for publishing...');
    for (const exchange of exchanges) {
      await declareExchangeAndQueue(
        exchange.name,
        exchange.name, // Create a default queue with same name
        exchange.type
      );
      logger.info(`Declared exchange ${exchange.name} with type ${exchange.type}`);
    }

    logger.info('Publisher infrastructure setup complete');
  } catch (error) {
    logger.error('Error setting up publisher infrastructure:', error);
    throw error;
  }
}

/**
 * Publish test messages to various exchanges
 */
async function publishTestMessages() {
  try {
    // 1. Publish test order messages
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
        exchangeName: 'orders',
        routingKey: 'orders.new',
        message: order,
        messageId: orderId,
        currentState: 'NEW',
        processingState: 'VALIDATING',
        nextState: 'VALIDATED',
        priority: PRIORITY.HIGH,
      });

      logger.info(`Published test order ${orderId}`);
    }

    // 2. Publish test notification messages
    for (let i = 0; i < 3; i++) {
      const notificationId = `test-notification-${Date.now()}-${i}`;
      const notification = {
        recipient: `test-user-${i}@example.com`,
        message: `Test notification message ${i}`,
        channel: i % 2 === 0 ? 'email' : 'sms',
      };

      await publisher.publish({
        exchangeName: 'notifications',
        routingKey: 'notifications.new',
        message: notification,
        messageId: notificationId,
        currentState: 'PENDING',
        processingState: 'SENDING',
        nextState: 'SENT',
        priority: PRIORITY.LOW,
      });

      logger.info(`Published test notification ${notificationId}`);
    }

    // 3. Publish test payment messages
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
        exchangeName: 'payments',
        routingKey: 'payments.new',
        message: payment,
        messageId: paymentId,
        currentState: 'PENDING',
        processingState: 'PROCESSING',
        nextState: 'AUTHORIZED',
        priority: PRIORITY.HIGH,
      });

      logger.info(`Published test payment ${paymentId}`);
    }

    // 4. Publish test shipment messages
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
        exchangeName: 'shipments',
        routingKey: 'shipments.new',
        message: shipment,
        messageId: shipmentId,
        currentState: 'CREATED',
        processingState: 'PICKING',
        nextState: 'PICKED',
        priority: PRIORITY.HIGH,
      });

      logger.info(`Published test shipment ${shipmentId}`);
    }

    // 5. Publish test inventory messages
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
        exchangeName: 'inventory',
        routingKey: 'inventory.allocate',
        message: inventory,
        messageId: inventoryId,
        currentState: 'AVAILABLE',
        processingState: 'ALLOCATING',
        nextState: 'ALLOCATED',
        priority: PRIORITY.MEDIUM,
      });

      logger.info(`Published test inventory update ${inventoryId}`);
    }

    // 6. Publish test audit messages
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
        exchangeName: 'audits',
        routingKey: 'audits.new',
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
 * Main publisher function
 */
async function main() {
  try {
    // Initialize core components
    await initializeCore();
    logger.info('Core initialized for publisher');

    // Setup publisher infrastructure
    await setupPublisherInfrastructure();

    // Wait a moment to ensure infrastructure is ready
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Publish test messages
    await publishTestMessages();

    // Exit after publishing
    logger.info('Publisher completed, exiting...');
    process.exit(0);
  } catch (error) {
    logger.error('Fatal error in publisher:', error);
    process.exit(1);
  }
}

// Start the publisher
main();
