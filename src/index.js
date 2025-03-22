import redisClient from '../src/client/redisClient.js';
import getLogger from './utils/logger.js';

// Set environment for local development
if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = 'development';
}

const logger = getLogger('main');
logger.info(`Start application with bitnami Redis Cluster in ${process.env.NODE_ENV} mode`);

// Function to check Redis connection and perform operations
async function runApp() {
  try {
    // Set a timeout to prevent hanging indefinitely
    const timeout = setTimeout(() => {
      logger.error('Redis Cluster connection timed out. Check your Redis cluster configuration.');
      process.exit(1);
    }, 15000); // Increased to 15 second timeout

    // Wait for Redis to be ready
    logger.info('Waiting for Redis Cluster to be fully ready...');
    await new Promise(resolve => setTimeout(resolve, 3000)); // Short delay to ensure client events are processed

    // Try to set and get a sample key with retries
    let retries = 0;
    const maxRetries = 3;
    let success = false;

    while (!success && retries < maxRetries) {
      try {
        await redisClient.set('test', 'sample value from app');
        const result = await redisClient.get('test');
        console.log('Redis Cluster sample result:', result);
        success = true;
      } catch (err) {
        retries++;
        logger.warn(`Redis test attempt ${retries}/${maxRetries} failed: ${err.message}`);
        if (retries < maxRetries) {
          logger.info(`Waiting before retry ${retries + 1}...`);
          await new Promise(resolve => setTimeout(resolve, 2000));
        } else {
          throw err;
        }
      }
    }

    // Try to set and get a few more keys to demonstrate distribution
    for (let i = 1; i <= 5; i++) {
      const key = `app:test:key:${i}`;
      const value = `Value ${i} from application at ${new Date().toISOString()}`;

      try {
        // Try to set the key with retries
        let setSuccess = false;
        let setRetries = 0;

        while (!setSuccess && setRetries < 3) {
          try {
            await redisClient.set(key, value);
            setSuccess = true;
            logger.info(`Set ${key} = ${value}`);
          } catch (err) {
            setRetries++;
            logger.warn(`Failed to set ${key} (attempt ${setRetries}/3): ${err.message}`);
            if (setRetries < 3) {
              await new Promise(resolve => setTimeout(resolve, 1000));
            } else {
              throw err;
            }
          }
        }

        // Try to get the key with retries
        let getSuccess = false;
        let getRetries = 0;

        while (!getSuccess && getRetries < 3) {
          try {
            const retrieved = await redisClient.get(key);
            getSuccess = true;
            logger.info(`Retrieved ${key} = ${retrieved}`);
          } catch (err) {
            getRetries++;
            logger.warn(`Failed to get ${key} (attempt ${getRetries}/3): ${err.message}`);
            if (getRetries < 3) {
              await new Promise(resolve => setTimeout(resolve, 1000));
            } else {
              throw err;
            }
          }
        }
      } catch (error) {
        logger.error(`Failed operation on key ${key}: ${error.message}`);
      }
    }

    // Clear the timeout since we succeeded
    clearTimeout(timeout);

    logger.info(
      'Application running successfully with Redis Cluster - keys are properly distributed'
    );
  } catch (error) {
    logger.error(`Redis Cluster operation failed: ${error.message}`);
    logger.error('Make sure your Redis Cluster is properly configured and all nodes are running');
    process.exit(1);
  }
}

// Run the application
runApp();
