// main app
import { consumer, initializeCore, shutdownCore } from './core/index.js';
import getLogger from './utils/logger.js';

// Constants for priority levels
const PRIORITY = {
  HIGH: 3,
  MEDIUM: 2,
  LOW: 1,
};

const logger = getLogger('app');

// In-memory fake database
const fakeDb = {
  orders: [],
  orderErrors: [],
  notifications: [],
  notificationErrors: [],
  payments: [],
  paymentErrors: [],
  shipments: [],
  shipmentErrors: [],
  inventory: [],
  inventoryErrors: [],
  audits: [],
  auditErrors: [],
};

// Fake database client
const fakeDbClient = {
  query: async (query, params) => {
    logger.debug(`FAKE DB QUERY: ${query}`);
    logger.debug(`PARAMS: ${JSON.stringify(params)}`);

    // Simulate database response based on query type
    if (query.includes('SELECT') && query.includes('orders')) {
      const [messageId, state] = params;
      const orders = fakeDb.orders.filter(
        o => o.order_id === messageId && (state ? o.state === state : true)
      );
      return { rows: orders };
    } else if (query.includes('SELECT') && query.includes('notifications')) {
      const [messageId, state] = params;
      const notifications = fakeDb.notifications.filter(
        n => n.notification_id === messageId && (state ? n.state === state : true)
      );
      return { rows: notifications };
    } else if (query.includes('SELECT') && query.includes('payments')) {
      const [messageId, state] = params;
      const payments = fakeDb.payments.filter(
        p => p.payment_id === messageId && (state ? p.state === state : true)
      );
      return { rows: payments };
    } else if (query.includes('SELECT') && query.includes('shipments')) {
      const [messageId, state] = params;
      const shipments = fakeDb.shipments.filter(
        s => s.shipment_id === messageId && (state ? s.state === state : true)
      );
      return { rows: shipments };
    } else if (query.includes('SELECT') && query.includes('inventory')) {
      const [messageId, state] = params;
      const inventory = fakeDb.inventory.filter(
        i => i.inventory_id === messageId && (state ? i.state === state : true)
      );
      return { rows: inventory };
    } else if (query.includes('SELECT') && query.includes('audits')) {
      const [messageId, state] = params;
      const audits = fakeDb.audits.filter(
        a => a.audit_id === messageId && (state ? a.state === state : true)
      );
      return { rows: audits };
    } else if (query.includes('UPDATE') && query.includes('orders')) {
      const [state, , messageId] = params;
      const orderIndex = fakeDb.orders.findIndex(o => o.order_id === messageId);
      if (orderIndex >= 0) {
        fakeDb.orders[orderIndex].state = state;
        fakeDb.orders[orderIndex].updated_at = new Date().toISOString();
      }
      return { affectedRows: orderIndex >= 0 ? 1 : 0 };
    } else if (query.includes('UPDATE') && query.includes('notifications')) {
      const [state, , messageId] = params;
      const notificationIndex = fakeDb.notifications.findIndex(
        n => n.notification_id === messageId
      );
      if (notificationIndex >= 0) {
        fakeDb.notifications[notificationIndex].state = state;
        fakeDb.notifications[notificationIndex].sent_at = new Date().toISOString();
      }
      return { affectedRows: notificationIndex >= 0 ? 1 : 0 };
    } else if (query.includes('UPDATE') && query.includes('payments')) {
      const [state, , messageId] = params;
      const paymentIndex = fakeDb.payments.findIndex(p => p.payment_id === messageId);
      if (paymentIndex >= 0) {
        fakeDb.payments[paymentIndex].state = state;
        fakeDb.payments[paymentIndex].updated_at = new Date().toISOString();
      }
      return { affectedRows: paymentIndex >= 0 ? 1 : 0 };
    } else if (query.includes('UPDATE') && query.includes('shipments')) {
      const [state, , messageId] = params;
      const shipmentIndex = fakeDb.shipments.findIndex(s => s.shipment_id === messageId);
      if (shipmentIndex >= 0) {
        fakeDb.shipments[shipmentIndex].state = state;
        fakeDb.shipments[shipmentIndex].updated_at = new Date().toISOString();
      }
      return { affectedRows: shipmentIndex >= 0 ? 1 : 0 };
    } else if (query.includes('UPDATE') && query.includes('inventory')) {
      const [state, , messageId] = params;
      const inventoryIndex = fakeDb.inventory.findIndex(i => i.inventory_id === messageId);
      if (inventoryIndex >= 0) {
        fakeDb.inventory[inventoryIndex].state = state;
        fakeDb.inventory[inventoryIndex].updated_at = new Date().toISOString();
      }
      return { affectedRows: inventoryIndex >= 0 ? 1 : 0 };
    } else if (query.includes('UPDATE') && query.includes('audits')) {
      const [state, , messageId] = params;
      const auditIndex = fakeDb.audits.findIndex(a => a.audit_id === messageId);
      if (auditIndex >= 0) {
        fakeDb.audits[auditIndex].state = state;
        fakeDb.audits[auditIndex].updated_at = new Date().toISOString();
      }
      return { affectedRows: auditIndex >= 0 ? 1 : 0 };
    } else if (query.includes('INSERT') && query.includes('order_errors')) {
      const [messageId, state, error, timestamp] = params;
      fakeDb.orderErrors.push({
        order_id: messageId,
        current_state: state,
        error,
        occurred_at: timestamp,
      });
      return { insertId: fakeDb.orderErrors.length };
    } else if (query.includes('INSERT') && query.includes('notification_errors')) {
      const [messageId, error, timestamp] = params;
      fakeDb.notificationErrors.push({
        notification_id: messageId,
        error,
        occurred_at: timestamp,
      });
      return { insertId: fakeDb.notificationErrors.length };
    } else if (query.includes('INSERT') && query.includes('payment_errors')) {
      const [messageId, error, timestamp] = params;
      fakeDb.paymentErrors.push({
        payment_id: messageId,
        error,
        occurred_at: timestamp,
      });
      return { insertId: fakeDb.paymentErrors.length };
    } else if (query.includes('INSERT') && query.includes('shipment_errors')) {
      const [messageId, error, timestamp] = params;
      fakeDb.shipmentErrors.push({
        shipment_id: messageId,
        error,
        occurred_at: timestamp,
      });
      return { insertId: fakeDb.shipmentErrors.length };
    } else if (query.includes('INSERT') && query.includes('inventory_errors')) {
      const [messageId, error, timestamp] = params;
      fakeDb.inventoryErrors.push({
        inventory_id: messageId,
        error,
        occurred_at: timestamp,
      });
      return { insertId: fakeDb.inventoryErrors.length };
    } else if (query.includes('INSERT') && query.includes('audit_errors')) {
      const [messageId, error, timestamp] = params;
      fakeDb.auditErrors.push({
        audit_id: messageId,
        error,
        occurred_at: timestamp,
      });
      return { insertId: fakeDb.auditErrors.length };
    }

    return { success: true };
  },
};

// Initialize fake database with records for each entity type
function initializeFakeDatabase() {
  const orderStates = ['NEW', 'VALIDATED', 'PAID', 'SHIPPED', 'DELIVERED'];
  const notificationStates = ['PENDING', 'SENT', 'FAILED'];
  const paymentStates = ['PENDING', 'AUTHORIZED', 'CAPTURED', 'REFUNDED', 'FAILED'];
  const shipmentStates = ['CREATED', 'PICKED', 'PACKED', 'SHIPPED', 'DELIVERED'];
  const inventoryStates = ['AVAILABLE', 'RESERVED', 'ALLOCATED', 'DEPLETED'];
  const auditStates = ['PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'];

  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 30); // Start from 30 days ago

  // Generate 100 records for each entity
  for (let i = 0; i < 100; i++) {
    const baseDate = new Date(startDate);
    baseDate.setHours(baseDate.getHours() + i * 2);

    // Create orders
    fakeDb.orders.push({
      order_id: `order-${i + 1000}`,
      customer_id: `customer-${1000 + Math.floor(Math.random() * 50)}`,
      state: orderStates[Math.floor(Math.random() * orderStates.length)],
      amount: Math.floor(Math.random() * 10000) / 100,
      created_at: baseDate.toISOString(),
      updated_at: baseDate.toISOString(),
    });

    // Create notifications
    fakeDb.notifications.push({
      notification_id: `notification-${i + 1000}`,
      recipient: `user${1000 + Math.floor(Math.random() * 50)}@example.com`,
      message: `Notification message ${i + 1000}`,
      channel: Math.random() > 0.5 ? 'email' : 'sms',
      state: notificationStates[Math.floor(Math.random() * notificationStates.length)],
      created_at: baseDate.toISOString(),
      sent_at: Math.random() > 0.3 ? baseDate.toISOString() : null,
    });

    // Create payments
    fakeDb.payments.push({
      payment_id: `payment-${i + 1000}`,
      order_id: `order-${1000 + Math.floor(Math.random() * 100)}`,
      amount: Math.floor(Math.random() * 10000) / 100,
      payment_method: ['credit_card', 'paypal', 'bank_transfer'][Math.floor(Math.random() * 3)],
      state: paymentStates[Math.floor(Math.random() * paymentStates.length)],
      created_at: baseDate.toISOString(),
      updated_at: baseDate.toISOString(),
    });

    // Create shipments
    fakeDb.shipments.push({
      shipment_id: `shipment-${i + 1000}`,
      order_id: `order-${1000 + Math.floor(Math.random() * 100)}`,
      carrier: ['fedex', 'ups', 'usps', 'dhl'][Math.floor(Math.random() * 4)],
      tracking_number: `TRACK-${Math.floor(Math.random() * 1000000)}`,
      state: shipmentStates[Math.floor(Math.random() * shipmentStates.length)],
      created_at: baseDate.toISOString(),
      updated_at: baseDate.toISOString(),
    });

    // Create inventory records
    fakeDb.inventory.push({
      inventory_id: `inventory-${i + 1000}`,
      product_id: `product-${1000 + Math.floor(Math.random() * 50)}`,
      quantity: Math.floor(Math.random() * 1000),
      location: `warehouse-${Math.floor(Math.random() * 5) + 1}`,
      state: inventoryStates[Math.floor(Math.random() * inventoryStates.length)],
      created_at: baseDate.toISOString(),
      updated_at: baseDate.toISOString(),
    });

    // Create audit records
    fakeDb.audits.push({
      audit_id: `audit-${i + 1000}`,
      entity_type: ['order', 'payment', 'shipment', 'inventory'][Math.floor(Math.random() * 4)],
      entity_id: `entity-${1000 + Math.floor(Math.random() * 100)}`,
      user_id: `user-${Math.floor(Math.random() * 20) + 1}`,
      action: ['create', 'update', 'delete', 'view'][Math.floor(Math.random() * 4)],
      state: auditStates[Math.floor(Math.random() * auditStates.length)],
      created_at: baseDate.toISOString(),
      updated_at: baseDate.toISOString(),
    });
  }

  logger.info(`Initialized fake database with:`);
  logger.info(`- ${fakeDb.orders.length} orders`);
  logger.info(`- ${fakeDb.notifications.length} notifications`);
  logger.info(`- ${fakeDb.payments.length} payments`);
  logger.info(`- ${fakeDb.shipments.length} shipments`);
  logger.info(`- ${fakeDb.inventory.length} inventory records`);
  logger.info(`- ${fakeDb.audits.length} audit records`);
}

// Fake business logic functions
const sendOrderValidationNotification = async orderId => {
  logger.info(`FAKE: Sending validation notification for order ${orderId}`);
  return { success: true };
};

const createShippingOrder = async orderId => {
  logger.info(`FAKE: Creating shipping order for order ${orderId}`);
  return { success: true, trackingNumber: `TRACK-${Math.floor(Math.random() * 10000)}` };
};

const sendPaymentConfirmation = async orderId => {
  logger.info(`FAKE: Sending payment confirmation for order ${orderId}`);
  return { success: true };
};

const sendPaymentFailureNotification = async (orderId, reason) => {
  logger.info(`FAKE: Sending payment failure notification for order ${orderId}: ${reason}`);
  return { success: true };
};

const processPayment = async paymentDetails => {
  logger.info(
    `FAKE: Processing payment for order ${paymentDetails.orderId} of ${paymentDetails.amount}`
  );

  // 10% chance of payment failure for testing error handlers
  if (Math.random() < 0.1) {
    return {
      status: 'failed',
      message: 'Insufficient funds',
    };
  }

  return {
    status: 'success',
    transactionId: `TRANS-${Date.now()}`,
    processedAt: new Date().toISOString(),
  };
};

const recordPaymentDetails = async (orderId, transactionId) => {
  logger.info(
    `FAKE: Recording payment details for order ${orderId}, transaction: ${transactionId}`
  );
  return { success: true };
};

const sendNotification = async (recipient, message, channel) => {
  logger.info(`FAKE: Sending ${channel} notification to ${recipient}: ${message}`);

  // 5% chance of notification failure
  if (Math.random() < 0.05) {
    return {
      success: false,
      error: `${channel} service unavailable`,
    };
  }

  return { success: true };
};

const allocateInventory = async (productId, quantity) => {
  logger.info(`FAKE: Allocating ${quantity} units of product ${productId}`);

  // 15% chance of inventory allocation failure
  if (Math.random() < 0.15) {
    return {
      success: false,
      error: 'Insufficient inventory',
    };
  }

  return { success: true, allocated: quantity };
};

const trackShipment = async trackingNumber => {
  logger.info(`FAKE: Tracking shipment ${trackingNumber}`);

  const statuses = ['PICKED_UP', 'IN_TRANSIT', 'OUT_FOR_DELIVERY', 'DELIVERED'];
  const randomStatus = statuses[Math.floor(Math.random() * statuses.length)];

  return {
    status: randomStatus,
    location: `Location-${Math.floor(Math.random() * 10)}`,
    timestamp: new Date().toISOString(),
  };
};

const logAuditEvent = async (entityType, entityId, action, userId) => {
  logger.info(`FAKE: Logging audit event for ${entityType} ${entityId}: ${action} by ${userId}`);
  return { success: true, auditId: `AUDIT-${Date.now()}` };
};

// Initialize the system
async function initialize() {
  try {
    // Initialize fake database before core system
    initializeFakeDatabase();

    // Initialize core system
    await initializeCore();
    logger.info('Core system initialized successfully');

    // 1. Register handler for orders queue
    consumer.register({
      queueName: 'q.high-priority-orders',
      currentState: 'NEW',
      processingState: 'VALIDATING',
      nextState: 'VALIDATED',
      bindingInfo: {
        exchangeName: 'ex.high-priority-orders',
        routingKey: 'rt.orders',
        exchangeType: 'direct',
      },
      priority: PRIORITY.HIGH,
      handler: async (order, metadata) => {
        logger.info(`Processing order validation for ${metadata.messageId}`);
        await sleep(getRandomInt(2500, 10_000));
        logger.info(`Order ${metadata.messageId} validated successfully`);
      },

      // Check if order is already in final state
      isCompletedProcess: async (messageId, currentState) => {
        try {
          const result = await fakeDbClient.query(
            'SELECT * FROM orders WHERE order_id = ? AND state = ?',
            [messageId, currentState]
          );

          if (result.rows.length > 0) {
            logger.info(`Order ${messageId} already in final state ${currentState}`);
            return result.rows[0];
          }
          return null;
        } catch (error) {
          logger.error(`Error checking order state: ${error.message}`);
          return null;
        }
      },

      // Handle successful state transition
      onCompletedProcess: async (messageId, currentState, nextState, metadata) => {
        try {
          // Update order state in database
          await fakeDbClient.query(
            'UPDATE orders SET state = ?, updated_at = ? WHERE order_id = ?',
            [nextState, new Date(), messageId]
          );

          // Additional business logic based on state
          if (nextState === 'VALIDATED') {
            await sendOrderValidationNotification(messageId);
          } else if (nextState === 'PAID') {
            await createShippingOrder(messageId);
            await sendPaymentConfirmation(messageId);
          }

          logger.info(`Order ${messageId} state updated from ${currentState} to ${nextState}`);
          return true;
        } catch (error) {
          logger.error(`Error updating order state: ${error.message}`);
          return false;
        }
      },

      // Handle state transition errors
      onErrorProcess: async (messageId, currentState, error, metadata) => {
        try {
          // Log the error
          await fakeDbClient.query(
            'INSERT INTO order_errors (order_id, current_state, error, occurred_at) VALUES (?, ?, ?, ?)',
            [messageId, currentState, error.message, new Date()]
          );

          // Update order status based on error
          if (metadata.processingState === 'PROCESSING_PAYMENT') {
            await fakeDbClient.query(
              'UPDATE orders SET state = ?, updated_at = ? WHERE order_id = ?',
              ['PAYMENT_FAILED', new Date(), messageId]
            );

            // Notify customer of payment failure
            await sendPaymentFailureNotification(messageId, error.message);
          }

          logger.error(`Order ${messageId} processing error: ${error.message}`);
          return true;
        } catch (dbError) {
          logger.error(`Error handling order error: ${dbError.message}`);
          return false;
        }
      },
    });

    // 2. Register handler for notifications queue
    consumer.register({
      queueName: 'q.notifications',
      currentState: 'PENDING',
      processingState: 'SENDING',
      nextState: 'SENT',
      bindingInfo: {
        exchangeName: 'ex.notifications',
        routingKey: 'rt.notifications',
        exchangeType: 'direct',
      },
      priority: PRIORITY.LOW,
      handler: async (notification, metadata) => {
        logger.info(`Sending notification ${metadata.messageId}`);

        await sleep(getRandomInt(2500, 10_000));

        // Send notification based on channel
        const result = await sendNotification(
          notification.recipient || 'user@example.com',
          notification.message || 'Default notification message',
          notification.channel || 'email'
        );

        if (!result.success) {
          throw new Error(`Failed to send notification: ${result.error}`);
        }

        logger.info(`Notification ${metadata.messageId} sent successfully`);
      },

      isCompletedProcess: async (messageId, currentState) => {
        // Implementation for checking notification state
        try {
          const result = await fakeDbClient.query(
            'SELECT * FROM notifications WHERE notification_id = ? AND state = ?',
            [messageId, currentState]
          );

          if (result.rows.length > 0) {
            logger.info(`Notification ${messageId} already in final state ${currentState}`);
            return result.rows[0];
          }
          return null;
        } catch (error) {
          logger.error(`Error checking notification state: ${error.message}`);
          return null;
        }
      },

      onCompletedProcess: async (messageId, currentState, nextState, metadata) => {
        // Implementation for updating notification state
        try {
          await fakeDbClient.query(
            'UPDATE notifications SET state = ?, sent_at = ? WHERE notification_id = ?',
            [nextState, new Date(), messageId]
          );
          logger.info(`Notification ${messageId} state updated to ${nextState}`);
          return true;
        } catch (error) {
          logger.error(`Error updating notification state: ${error.message}`);
          return false;
        }
      },

      onErrorProcess: async (messageId, currentState, error, metadata) => {
        // Implementation for handling notification errors
        try {
          await fakeDbClient.query(
            'INSERT INTO notification_errors (notification_id, error, occurred_at) VALUES (?, ?, ?)',
            [messageId, error.message, new Date()]
          );

          // Update notification status for retry
          await fakeDbClient.query(
            'UPDATE notifications SET state = ?, retry_count = retry_count + 1 WHERE notification_id = ?',
            ['FAILED', messageId]
          );

          logger.error(`Notification ${messageId} send error: ${error.message}`);
          return true;
        } catch (dbError) {
          logger.error(`Error handling notification error: ${dbError.message}`);
          return false;
        }
      },
    });

    // 3. Register handler for payments queue
    consumer.register({
      queueName: 'q.payments',
      currentState: 'PENDING',
      processingState: 'PROCESSING',
      nextState: 'AUTHORIZED',
      // Add binding information
      bindingInfo: {
        exchangeName: 'ex.payments',
        routingKey: 'rt.payments',
        exchangeType: 'direct',
      },
      priority: PRIORITY.HIGH,
      handler: async (payment, metadata) => {
        logger.info(`Processing payment ${metadata.messageId} for order ${payment.orderId}`);

        await sleep(getRandomInt(2500, 10_000));
        if (!payment.orderId || !payment.amount) {
          throw new Error('Payment must have orderId and amount');
        }

        const paymentResult = await processPayment({
          orderId: payment.orderId,
          amount: payment.amount,
          method: payment.paymentMethod,
        });

        if (paymentResult.status !== 'success') {
          throw new Error(`Payment processing failed: ${paymentResult.message}`);
        }

        // Record transaction details for later reference
        await recordPaymentDetails(payment.orderId, paymentResult.transactionId);

        logger.info(`Payment ${metadata.messageId} processed successfully`);
      },

      isCompletedProcess: async (messageId, currentState) => {
        try {
          const result = await fakeDbClient.query(
            'SELECT * FROM payments WHERE payment_id = ? AND state = ?',
            [messageId, currentState]
          );

          if (result.rows.length > 0) {
            logger.info(`Payment ${messageId} already in final state ${currentState}`);
            return result.rows[0];
          }
          return null;
        } catch (error) {
          logger.error(`Error checking payment state: ${error.message}`);
          return null;
        }
      },

      onCompletedProcess: async (messageId, currentState, nextState, metadata) => {
        try {
          await fakeDbClient.query(
            'UPDATE payments SET state = ?, updated_at = ? WHERE payment_id = ?',
            [nextState, new Date(), messageId]
          );

          logger.info(`Payment ${messageId} state updated from ${currentState} to ${nextState}`);
          return true;
        } catch (error) {
          logger.error(`Error updating payment state: ${error.message}`);
          return false;
        }
      },

      onErrorProcess: async (messageId, currentState, error, metadata) => {
        try {
          await fakeDbClient.query(
            'INSERT INTO payment_errors (payment_id, error, occurred_at) VALUES (?, ?, ?)',
            [messageId, error.message, new Date()]
          );

          // Update payment to failed state
          await fakeDbClient.query(
            'UPDATE payments SET state = ?, updated_at = ? WHERE payment_id = ?',
            ['FAILED', new Date(), messageId]
          );

          logger.error(`Payment ${messageId} processing error: ${error.message}`);
          return true;
        } catch (dbError) {
          logger.error(`Error handling payment error: ${dbError.message}`);
          return false;
        }
      },
    });

    // 4. Register handler for shipments queue
    consumer.register({
      queueName: 'q.shipments',
      currentState: 'CREATED',
      processingState: 'PICKING',
      nextState: 'PICKED',
      // Add binding information
      bindingInfo: {
        exchangeName: 'ex.shipments',
        routingKey: 'rt.shipments',
        exchangeType: 'direct',
      },
      priority: PRIORITY.HIGH,
      handler: async (shipment, metadata) => {
        logger.info(`Processing shipment ${metadata.messageId} for order ${shipment.orderId}`);
        await sleep(getRandomInt(2500, 10_000));
        if (!shipment.orderId || !shipment.items || shipment.items.length === 0) {
          throw new Error('Shipment must have orderId and items');
        }

        // Process each item in the shipment
        for (const item of shipment.items) {
          // Allocate inventory for the item
          const allocationResult = await allocateInventory(item.productId, item.quantity);

          if (!allocationResult.success) {
            throw new Error(
              `Failed to allocate inventory for product ${item.productId}: ${allocationResult.error}`
            );
          }
        }

        logger.info(`Shipment ${metadata.messageId} items picked successfully`);
      },

      isCompletedProcess: async (messageId, currentState) => {
        try {
          const result = await fakeDbClient.query(
            'SELECT * FROM shipments WHERE shipment_id = ? AND state = ?',
            [messageId, currentState]
          );

          if (result.rows.length > 0) {
            logger.info(`Shipment ${messageId} already in final state ${currentState}`);
            return result.rows[0];
          }
          return null;
        } catch (error) {
          logger.error(`Error checking shipment state: ${error.message}`);
          return null;
        }
      },

      onCompletedProcess: async (messageId, currentState, nextState, metadata) => {
        try {
          await fakeDbClient.query(
            'UPDATE shipments SET state = ?, updated_at = ? WHERE shipment_id = ?',
            [nextState, new Date(), messageId]
          );

          logger.info(`Shipment ${messageId} state updated from ${currentState} to ${nextState}`);
          return true;
        } catch (error) {
          logger.error(`Error updating shipment state: ${error.message}`);
          return false;
        }
      },

      onErrorProcess: async (messageId, currentState, error, metadata) => {
        try {
          await fakeDbClient.query(
            'INSERT INTO shipment_errors (shipment_id, error, occurred_at) VALUES (?, ?, ?)',
            [messageId, error.message, new Date()]
          );

          logger.error(`Shipment ${messageId} processing error: ${error.message}`);
          return true;
        } catch (dbError) {
          logger.error(`Error handling shipment error: ${dbError.message}`);
          return false;
        }
      },
    });

    // 5. Register handler for inventory queue
    consumer.register({
      queueName: 'q.inventory',
      currentState: 'AVAILABLE',
      processingState: 'ALLOCATING',
      nextState: 'ALLOCATED',
      // Add binding information
      bindingInfo: {
        exchangeName: 'ex.inventory',
        routingKey: 'rt.inventory',
        exchangeType: 'direct',
      },
      priority: PRIORITY.MEDIUM,
      handler: async (inventory, metadata) => {
        logger.info(
          `Processing inventory allocation ${metadata.messageId} for product ${inventory.productId}`
        );
        await sleep(getRandomInt(2500, 10_000));
        if (!inventory.productId || !inventory.quantity) {
          throw new Error('Inventory update must have productId and quantity');
        }

        const allocationResult = await allocateInventory(inventory.productId, inventory.quantity);

        if (!allocationResult.success) {
          throw new Error(`Failed to allocate inventory: ${allocationResult.error}`);
        }

        // Log the allocation as an audit event
        await logAuditEvent('inventory', inventory.productId, 'allocate', 'system');

        logger.info(`Inventory ${metadata.messageId} successfully allocated`);
      },

      isCompletedProcess: async (messageId, currentState) => {
        try {
          const result = await fakeDbClient.query(
            'SELECT * FROM inventory WHERE inventory_id = ? AND state = ?',
            [messageId, currentState]
          );

          if (result.rows.length > 0) {
            logger.info(`Inventory ${messageId} already in final state ${currentState}`);
            return result.rows[0];
          }
          return null;
        } catch (error) {
          logger.error(`Error checking inventory state: ${error.message}`);
          return null;
        }
      },

      onCompletedProcess: async (messageId, currentState, nextState, metadata) => {
        try {
          await fakeDbClient.query(
            'UPDATE inventory SET state = ?, updated_at = ? WHERE inventory_id = ?',
            [nextState, new Date(), messageId]
          );

          logger.info(`Inventory ${messageId} state updated from ${currentState} to ${nextState}`);
          return true;
        } catch (error) {
          logger.error(`Error updating inventory state: ${error.message}`);
          return false;
        }
      },

      onErrorProcess: async (messageId, currentState, error, metadata) => {
        try {
          await fakeDbClient.query(
            'INSERT INTO inventory_errors (inventory_id, error, occurred_at) VALUES (?, ?, ?)',
            [messageId, error.message, new Date()]
          );

          logger.error(`Inventory ${messageId} processing error: ${error.message}`);
          return true;
        } catch (dbError) {
          logger.error(`Error handling inventory error: ${dbError.message}`);
          return false;
        }
      },
    });

    // 6. Register handler for audit queue
    consumer.register({
      queueName: 'q.audit',
      currentState: 'PENDING',
      processingState: 'PROCESSING',
      nextState: 'COMPLETED',
      // Add binding information
      bindingInfo: {
        exchangeName: 'ex.audits',
        routingKey: 'rt.audits',
        exchangeType: 'direct',
      },
      priority: PRIORITY.LOW,
      handler: async (audit, metadata) => {
        logger.info(`Processing audit log ${metadata.messageId}`);
        await sleep(getRandomInt(2500, 10_000));
        if (!audit.entityType || !audit.entityId || !audit.action) {
          throw new Error('Audit log must have entityType, entityId, and action');
        }

        // Log the audit event
        await logAuditEvent(
          audit.entityType,
          audit.entityId,
          audit.action,
          audit.userId || 'system'
        );

        logger.info(`Audit log ${metadata.messageId} processed successfully`);
      },

      isCompletedProcess: async (messageId, currentState) => {
        try {
          const result = await fakeDbClient.query(
            'SELECT * FROM audits WHERE audit_id = ? AND state = ?',
            [messageId, currentState]
          );

          if (result.rows.length > 0) {
            logger.info(`Audit ${messageId} already in final state ${currentState}`);
            return result.rows[0];
          }
          return null;
        } catch (error) {
          logger.error(`Error checking audit state: ${error.message}`);
          return null;
        }
      },

      onCompletedProcess: async (messageId, currentState, nextState, metadata) => {
        try {
          await fakeDbClient.query(
            'UPDATE audits SET state = ?, updated_at = ? WHERE audit_id = ?',
            [nextState, new Date(), messageId]
          );

          logger.info(`Audit ${messageId} state updated from ${currentState} to ${nextState}`);
          return true;
        } catch (error) {
          logger.error(`Error updating audit state: ${error.message}`);
          return false;
        }
      },

      onErrorProcess: async (messageId, currentState, error, metadata) => {
        try {
          await fakeDbClient.query(
            'INSERT INTO audit_errors (audit_id, error, occurred_at) VALUES (?, ?, ?)',
            [messageId, error.message, new Date()]
          );

          logger.error(`Audit ${messageId} processing error: ${error.message}`);
          return true;
        } catch (dbError) {
          logger.error(`Error handling audit error: ${dbError.message}`);
          return false;
        }
      },
    });

    logger.info('Queue handlers and callbacks registered');

    // Display database stats every minute
    setInterval(() => {
      logger.info('========== Fake Database Stats ==========');
      logger.info(`Orders: ${fakeDb.orders.length} records, Errors: ${fakeDb.orderErrors.length}`);
      logger.info(
        `Notifications: ${fakeDb.notifications.length} records, Errors: ${fakeDb.notificationErrors.length}`
      );
      logger.info(
        `Payments: ${fakeDb.payments.length} records, Errors: ${fakeDb.paymentErrors.length}`
      );
      logger.info(
        `Shipments: ${fakeDb.shipments.length} records, Errors: ${fakeDb.shipmentErrors.length}`
      );
      logger.info(
        `Inventory: ${fakeDb.inventory.length} records, Errors: ${fakeDb.inventoryErrors.length}`
      );
      logger.info(`Audits: ${fakeDb.audits.length} records, Errors: ${fakeDb.auditErrors.length}`);
      logger.info('=========================================');
    }, 60000);

    // Display consumer stats every 30 seconds
    setInterval(() => {
      const stats = consumer.getStats();
      logger.info('Consumer stats:', JSON.stringify(stats, null, 2));
    }, 30000);
  } catch (error) {
    logger.error('Error initializing application:', error);
  }
}

function sleep(ms) {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

function getRandomInt(min, max) {
  const minCeiled = Math.ceil(min);
  const maxFloored = Math.floor(max);
  return Math.floor(Math.random() * (maxFloored - minCeiled) + minCeiled);
}

// Run the application
initialize().catch(err => {
  logger.error('Unhandled error during initialization:', err);
  process.exit(1);
});

// Handle graceful shutdown
process.on('SIGINT', async () => {
  logger.info('Shutdown signal received');
  try {
    await shutdownCore();
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown:', error);
    process.exit(1);
  }
});
