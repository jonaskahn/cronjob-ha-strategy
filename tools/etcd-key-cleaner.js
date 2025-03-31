import { Etcd3 } from 'etcd3';
import getLogger from '../src/utils/logger.js';

const logger = getLogger('tools/etcd-key-cleaner');

async function deleteAllKeys() {
  const client = new Etcd3({
    hosts: ['localhost:2379', 'localhost:2380', 'localhost:2381'],
  });

  try {
    const allKeys = await client.getAll().keys();

    if (allKeys.length === 0) {
      logger.info('No keys found in etcd');
      return;
    }
    
    for (const key of allKeys) {
      await client.delete().key(key);
      logger.info(`Deleted key: ${key}`);
    }

    logger.info(`Successfully deleted ${allKeys.length} keys`);
  } catch (error) {
    logger.error('Error deleting keys:', error);
  } finally {
    await client.close();
  }
}

// Run the cleanup
deleteAllKeys();
