import Redis from 'ioredis';
import getLogger from '../../src/utils/logger.js';

const logger = getLogger('sample-direct-redis');
logger.info('Starting direct Redis connection sample');

// Function to sample Redis connectivity directly to each node
async function testDirectRedisConnection() {
  const ports = [6379, 6380, 6381];
  const connections = [];

  try {
    // Connect to each Redis node directly
    for (const port of ports) {
      logger.info(`Connecting directly to Redis node at localhost:${port}`);

      const redis = new Redis({
        host: 'localhost',
        port: port,
        connectTimeout: 5000,
        maxRetriesPerRequest: 2,
      });

      // Setup basic event handlers
      redis.on('connect', () => {
        logger.info(`Connected to Redis at localhost:${port}`);
      });

      redis.on('error', err => {
        logger.error(`Redis error for localhost:${port}: ${err.message}`);
      });

      connections.push({ port, client: redis });
    }

    // Wait for connections to be established
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Try to get cluster info from each node
    for (const conn of connections) {
      try {
        logger.info(`Testing cluster info on localhost:${conn.port}`);
        const info = await conn.client.cluster('info');
        logger.info(`Cluster info from localhost:${conn.port}: ${info}`);

        // Test basic key-value operations
        await conn.client.set(`test-key-${conn.port}`, `value-${conn.port}`);
        const value = await conn.client.get(`test-key-${conn.port}`);
        logger.info(`Set/Get on localhost:${conn.port}: ${value}`);
      } catch (err) {
        logger.error(`Failed to get cluster info from localhost:${conn.port}: ${err.message}`);
      }
    }

    // Clean up
    for (const conn of connections) {
      await conn.client.quit();
    }

    logger.info('Direct Redis connection sample completed');
  } catch (error) {
    logger.error(`Direct Redis test failed: ${error.message}`);
    logger.error(error.stack);
  }
}

// Run the sample
testDirectRedisConnection();
