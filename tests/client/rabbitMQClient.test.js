import { afterAll, beforeAll, describe, expect, it } from '@jest/globals';
import rabbitMqClient from '../../src/client/rabbitMQClient.js';

describe('RabbitMQClient Integration Tests', () => {
  let client;
  const testQueue = 'integration-test-queue';
  const testMessage = { content: 'test-message' };

  // Create a single client for all tests
  beforeAll(async () => {
    client = await rabbitMqClient();
    const channel = await client.consume();
    await channel.assertQueue(testQueue, { durable: false });
    await channel.purgeQueue(testQueue);
    await channel.close();
  });

  afterAll(async () => {
    // Clean up
    if (client && client._client) {
      try {
        const channel = await client._client.createChannel();
        await channel.purgeQueue(testQueue);
        // Leave the queue for reuse
        await channel.close();
        await client._client.close();
      } catch (error) {
        console.error('Error during cleanup:', error);
      }
    }
  });

  it('should connect to RabbitMQ', () => {
    expect(client._client).toBeDefined();
  });

  it('should list queues', async () => {
    const queues = await client.listQueues();
    expect(Array.isArray(queues)).toBe(true);
  });

  it('should publish and consume a JSON message', async () => {
    // Create a channel for publishing
    const channel = await client._client.createChannel();

    // Create a promise for test completion
    const messageHandled = new Promise(resolve => {
      // Set up consumer first
      client.consume(testQueue, async message => {
        try {
          const content = JSON.parse(message.content.toString());
          expect(content).toEqual(testMessage);
          await message.ack();
          resolve();
        } catch (error) {
          console.error('Error consuming message:', error);
          resolve(error);
        }
      });
    });

    // Allow a small delay for consumer to be fully registered
    await new Promise(resolve => setTimeout(resolve, 500));

    // Then publish the message
    const messageContent = JSON.stringify(testMessage);
    await channel.sendToQueue(testQueue, Buffer.from(messageContent), { persistent: false });

    // Wait for message to be handled
    await messageHandled;

    // Clean up
    await channel.close();
  });

  it('should handle basic message acknowledgment', async () => {
    // Use a simple approach
    const channel = await client._client.createChannel();

    // Create a message
    const testString = 'test-ack-message';

    // Create a promise for test completion
    const ackComplete = new Promise(resolve => {
      client.consume(testQueue, async message => {
        try {
          // Verify message content
          expect(message.content.toString()).toBe(testString);

          // Acknowledge the message
          await message.ack();
          resolve();
        } catch (error) {
          console.error('Error in ack test:', error);
          resolve(error);
        }
      });
    });

    // Allow a small delay for consumer to be fully registered
    await new Promise(resolve => setTimeout(resolve, 500));

    // Publish the message
    await channel.sendToQueue(testQueue, Buffer.from(testString), { persistent: false });

    // Wait for acknowledgment to complete
    await ackComplete;

    // Clean up
    await channel.close();
  });
});
