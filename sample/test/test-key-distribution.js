import Redis from 'ioredis';
import getLogger from '../../src/utils/logger.js';

const logger = getLogger('sample-key-distribution');
logger.info('Starting Redis Cluster key distribution sample');

// Function to sample if keys are properly accessible across the cluster
async function testKeyDistribution() {
  const ports = [6379, 6380, 6381];
  const connections = [];
  const testKeys = [
    'sample-key-0', // Will likely be stored on one node
    'sample-key-1000', // Will likely be stored on a different node
    'sample-key-5000', // Will likely be stored on a different node
    'sample-key-10000', // Will likely be stored on a different node
  ];

  try {
    // Connect to each Redis node directly
    for (const port of ports) {
      logger.info(`Connecting directly to Redis node at localhost:${port}`);

      const redis = new Redis({
        host: 'localhost',
        port: port,
        connectTimeout: 5000,
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

    // First set all sample keys from node 1 (primary node)
    logger.info('--- Setting sample keys from primary node ---');
    const primaryNode = connections[0].client;

    for (const key of testKeys) {
      try {
        const value = `Value for ${key} set at ${new Date().toISOString()}`;
        await primaryNode.set(key, value);
        logger.info(`Set key "${key}" on primary node with value: "${value}"`);
      } catch (err) {
        logger.error(`Failed to set key ${key}: ${err.message}`);
      }
    }

    // Now try to get the keys from each node
    logger.info('--- Getting keys from each node ---');

    for (const conn of connections) {
      logger.info(`\nTesting key retrieval from localhost:${conn.port}:`);

      for (const key of testKeys) {
        try {
          const value = await conn.client.get(key);
          if (value) {
            logger.info(
              `Node ${conn.port}: Successfully retrieved key "${key}" with value: "${value}"`
            );
          } else {
            logger.error(`Node ${conn.port}: Key "${key}" not found or returned null`);
          }
        } catch (err) {
          if (err.message.includes('MOVED')) {
            // This is expected in a Redis Cluster - the key is on another node
            const redirectMatch = err.message.match(/MOVED \d+ ([0-9.]+):(\d+)/);
            if (redirectMatch) {
              logger.info(
                `Node ${conn.port}: Key "${key}" is located on node ${redirectMatch[1]}:${redirectMatch[2]}`
              );
            } else {
              logger.info(`Node ${conn.port}: Key "${key}" is located on another node`);
            }
          } else {
            logger.error(`Node ${conn.port}: Error retrieving key "${key}": ${err.message}`);
          }
        }
      }
    }

    // Now sample with the Cluster client to ensure it can access all keys
    logger.info('\n--- Testing key retrieval using Cluster client ---');

    // Create a Redis Cluster client
    const clusterClient = new Redis.Cluster(
      [
        { host: 'localhost', port: 6379 },
        { host: 'localhost', port: 6380 },
        { host: 'localhost', port: 6381 },
      ],
      {
        natMap: {
          '172.19.0.3:6379': { host: 'localhost', port: 6379 },
          '172.19.0.2:6379': { host: 'localhost', port: 6380 },
          '172.19.0.4:6379': { host: 'localhost', port: 6381 },
        },
        redisOptions: {
          connectTimeout: 5000,
        },
      }
    );

    for (const key of testKeys) {
      try {
        const value = await clusterClient.get(key);
        logger.info(`Cluster client: Successfully retrieved key "${key}" with value: "${value}"`);
      } catch (err) {
        logger.error(`Cluster client: Failed to get key "${key}": ${err.message}`);
      }
    }

    // Clean up
    for (const conn of connections) {
      await conn.client.quit();
    }
    await clusterClient.quit();

    logger.info('Redis Cluster key distribution sample completed');
  } catch (error) {
    logger.error(`Test failed: ${error.message}`);
    logger.error(error.stack);
  }
}

// Run the sample
testKeyDistribution();
