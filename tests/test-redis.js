import Redis from 'ioredis';
import getLoggerInstance from '../src/utils/logger.js';

const logger = getLoggerInstance('test-redis');
logger.info('Starting Redis Cluster connection test');

// Function to test Redis connectivity
async function testRedisCluster() {
  try {
    // Get Redis hosts from environment or use default
    const redisHosts = process.env.CHJS_REDIS_CLUSTER_HOST || 'localhost:6379,localhost:6380,localhost:6381';
    
    // Parse the hosts
    const clusterNodes = redisHosts.split(',').map(x => {
      const [host, port] = x.split(':');
      return { host, port: Number(port) };
    });
    
    logger.info(`Connecting to Redis Cluster with nodes: ${JSON.stringify(clusterNodes)}`);
    
    // Create a Redis Cluster client
    const redis = new Redis.Cluster(clusterNodes, {
      enableReadyCheck: true,
      scaleReads: 'all',
      maxRedirections: 16,
      // Critical for Docker: Map internal IPs to localhost addresses
      natMap: {
        '172.19.0.3:6379': { host: 'localhost', port: 6379 },
        '172.19.0.2:6379': { host: 'localhost', port: 6380 },
        '172.19.0.4:6379': { host: 'localhost', port: 6381 }
      },
      redisOptions: {
        connectTimeout: 5000
      }
    });
    
    // Setup event handlers
    redis.on('connect', () => {
      logger.info('Redis Cluster connected');
    });
    
    redis.on('error', (err) => {
      logger.error(`Redis Cluster error: ${err.message}`);
    });
    
    redis.on('node error', (err, node) => {
      if (node && node.options) {
        logger.error(`Redis Cluster Node ${node.options.host}:${node.options.port} Error: ${err.message}`);
      } else {
        logger.error(`Redis Cluster Node Error: ${err.message}`);
      }
    });
    
    // Set a test key
    logger.info('Setting test key in cluster');
    await redis.set('test-key', 'Hello Redis Cluster!');
    
    // Get the test key
    const value = await redis.get('test-key');
    logger.info(`Retrieved test key value: ${value}`);
    
    // Get cluster info
    logger.info('Cluster info:');
    const nodes = redis.nodes('all');
    logger.info(`Cluster has ${nodes.length} nodes`);
    
    // Clean up
    await redis.quit();
    logger.info('Redis Cluster test completed successfully');
  } catch (error) {
    logger.error(`Redis Cluster test failed: ${error.message}`);
    logger.error(error.stack);
    process.exit(1);
  }
}

// Run the test
testRedisCluster(); 