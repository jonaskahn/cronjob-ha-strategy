import redisClient from '../src/client/redisClient.js';
import getLoggerInstance from '../src/utils/logger.js';

const logger = getLoggerInstance('test-key-access');

// Function to check if a key is properly accessible via Redis Cluster
async function checkKeyAccess(keyNames) {
  try {
    logger.info('Starting Redis Cluster key access check');
    logger.info(`Keys to check: ${keyNames.join(', ')}`);
    
    // Check if keys exist
    for (const key of keyNames) {
      try {
        const exists = await redisClient.exists(key);
        if (exists) {
          logger.info(`Key "${key}" exists in the cluster`);
          
          // Try to get its value
          const value = await redisClient.get(key);
          logger.info(`Value for key "${key}": "${value}"`);
        } else {
          logger.warn(`Key "${key}" does not exist in the cluster`);
          
          // Set a value for this key
          const newValue = `Value set at ${new Date().toISOString()}`;
          await redisClient.set(key, newValue);
          logger.info(`Created key "${key}" with value: "${newValue}"`);
          
          // Verify it was set correctly
          const savedValue = await redisClient.get(key);
          logger.info(`Verified key "${key}" was set with value: "${savedValue}"`);
        }
      } catch (error) {
        logger.error(`Error handling key "${key}": ${error.message}`);
      }
    }
    
    logger.info('Key access check completed');
  } catch (error) {
    logger.error(`Test failed: ${error.message}`);
    logger.error(error.stack);
  }
}

// Keys to check - default if none provided
const defaultKeys = ['test', 'cluster:health'];

// Check if keys were provided as arguments
const userKeys = process.argv.slice(2);
const keysToCheck = userKeys.length > 0 ? userKeys : defaultKeys;

// Run the test
checkKeyAccess(keysToCheck).catch(err => {
  logger.error('Unhandled error in key access check:', err);
  process.exit(1);
}); 