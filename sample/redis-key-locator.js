import Redis from 'ioredis';
import readline from 'readline';

// Color output
const colors = {
  green: text => `\x1b[32m${text}\x1b[0m`,
  yellow: text => `\x1b[33m${text}\x1b[0m`,
  blue: text => `\x1b[34m${text}\x1b[0m`,
  magenta: text => `\x1b[35m${text}\x1b[0m`,
  cyan: text => `\x1b[36m${text}\x1b[0m`,
  red: text => `\x1b[31m${text}\x1b[0m`,
};

// NAT mapping to convert internal IPs to localhost ports
const natMapping = {
  '172.18.0.2:6379': { host: 'localhost', port: 6379 },
  '172.18.0.3:6379': { host: 'localhost', port: 6380 },
  '172.18.0.4:6379': { host: 'localhost', port: 6381 },
  '172.18.0.5:6379': { host: 'localhost', port: 6382 },
  '172.18.0.6:6379': { host: 'localhost', port: 6383 },
  '172.18.0.7:6379': { host: 'localhost', port: 6384 },
};

// Configuration for Redis Cluster
const clusterNodes = [
  { host: 'localhost', port: 6379 },
  { host: 'localhost', port: 6380 },
  { host: 'localhost', port: 6381 },
  { host: 'localhost', port: 6382 },
  { host: 'localhost', port: 6383 },
  { host: 'localhost', port: 6384 },
];

// Create a Redis cluster client
const cluster = new Redis.Cluster(clusterNodes, {
  redisOptions: {
    showFriendlyErrorStack: true,
    connectTimeout: 10000,
    commandTimeout: 10000,
  },
  scaleReads: 'all',
  maxRedirections: 16,
  natMap: natMapping,
});

// Create the readline interface
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

// Function to calculate CRC16 hash (similar to Redis's implementation)
function crc16(str) {
  let crc = 0;
  for (let i = 0; i < str.length; i++) {
    crc = ((crc << 8) & 0xffff) ^ str.charCodeAt(i);
    for (let j = 0; j < 8; j++) {
      crc = ((crc << 1) & 0xffff) ^ (crc & 0x8000 ? 0x1021 : 0);
    }
  }
  return crc;
}

// Function to calculate keyslot for a key (simplified approximation)
function calculateSlot(key) {
  // Extract hashTag if exists {hashTag}
  let hashTag = key;
  const startBrace = key.indexOf('{');
  const endBrace = key.indexOf('}', startBrace);

  if (startBrace !== -1 && endBrace !== -1 && startBrace + 1 < endBrace) {
    hashTag = key.substring(startBrace + 1, endBrace);
  }

  // Use CRC16 modulo 16384 to simulate Redis keyslot algorithm
  return crc16(hashTag) % 16384;
}

// Function to get the cluster nodes information
async function getClusterNodes() {
  const nodeInfo = {};

  try {
    // Using a direct connection to get CLUSTER NODES info
    const singleClient = new Redis(6379, 'localhost');
    const nodesStr = await singleClient.cluster('NODES');
    singleClient.disconnect();

    const lines = nodesStr.split('\n');
    for (const line of lines) {
      if (!line) continue;

      const parts = line.split(' ');
      const nodeId = parts[0];
      const addrInfo = parts[1].split('@')[0];

      // Check if master
      const isMaster = parts[2].includes('master');

      // Get slots info from the line
      const slotsInfo = [];
      for (let i = 8; i < parts.length; i++) {
        if (parts[i].includes('-')) {
          const [start, end] = parts[i].split('-');
          slotsInfo.push({ start: parseInt(start), end: parseInt(end) });
        }
      }

      // Map internal IP:port to localhost:port if needed
      let displayAddr = addrInfo;
      if (natMapping[addrInfo]) {
        const nat = natMapping[addrInfo];
        displayAddr = `${nat.host}:${nat.port}`;
      }

      nodeInfo[nodeId] = {
        id: nodeId,
        addr: addrInfo,
        displayAddr,
        isMaster,
        slots: slotsInfo,
      };
    }
  } catch (err) {
    console.error('Error getting cluster nodes:', err);
  }

  return nodeInfo;
}

// Function to find which node holds a key
async function findKeyLocation(key) {
  console.log(colors.blue(`\n[INFO] Finding location for key: ${colors.cyan(key)}`));

  try {
    // Get the estimated key slot
    const estimatedSlot = calculateSlot(key);
    console.log(colors.blue(`[INFO] Estimated key slot: ${colors.cyan(estimatedSlot)}`));

    // Get the actual key slot from Redis
    const actualSlot = await cluster.cluster('KEYSLOT', key);
    console.log(colors.blue(`[INFO] Actual Redis KEYSLOT: ${colors.cyan(actualSlot)}`));

    // Get cluster nodes information
    const nodesInfo = await getClusterNodes();

    // Find which node is responsible for this slot
    let targetNode = null;
    for (const nodeId in nodesInfo) {
      const node = nodesInfo[nodeId];

      if (node.slots) {
        for (const slotRange of node.slots) {
          if (actualSlot >= slotRange.start && actualSlot <= slotRange.end) {
            targetNode = node;
            break;
          }
        }
      }

      if (targetNode) break;
    }

    if (targetNode) {
      console.log(colors.green(`\n[SUCCESS] Key "${key}" is mapped to slot ${actualSlot}`));
      console.log(
        colors.green(
          `[SUCCESS] This slot is managed by node at ${colors.yellow(targetNode.displayAddr)}`
        )
      );
      console.log(
        colors.green(`[SUCCESS] Node ID: ${colors.yellow(targetNode.id.substring(0, 8))}...`)
      );
      console.log(
        colors.green(
          `[SUCCESS] Node is a ${targetNode.isMaster ? colors.yellow('MASTER') : colors.magenta('REPLICA')}`
        )
      );

      // Set a sample value
      const testValue = `Value set at ${new Date().toISOString()}`;
      await cluster.set(key, testValue);
      console.log(colors.green(`\n[SUCCESS] Set key "${key}" to: ${colors.cyan(testValue)}`));

      // Get the value back
      const getValue = await cluster.get(key);
      console.log(colors.green(`[SUCCESS] Retrieved value: ${colors.cyan(getValue)}`));
    } else {
      console.log(
        colors.red(`\n[ERROR] Could not determine which node manages slot ${actualSlot}`)
      );
    }
  } catch (err) {
    console.error(colors.red(`\n[ERROR] Error finding key location: ${err.message}`));
  }
}

// Interactive command loop
async function startCommandLoop() {
  console.log(colors.yellow('\n=== Redis Cluster Key Locator Tool ==='));
  console.log(
    colors.yellow('This tool helps you see which node in your Redis Cluster stores a given key')
  );
  console.log(colors.yellow('Use CTRL+C to exit\n'));

  await promptForKey();
}

// Prompt for a key
function promptForKey() {
  rl.question(colors.cyan('Enter a key to locate (or "exit" to quit): '), async input => {
    if (input.toLowerCase() === 'exit' || input.toLowerCase() === 'quit') {
      await cleanup();
      return;
    }

    await findKeyLocation(input);
    promptForKey();
  });
}

// Cleanup function
async function cleanup() {
  console.log(colors.yellow('\nDisconnecting from Redis Cluster...'));
  await cluster.quit();
  rl.close();
  console.log(colors.yellow('Goodbye!'));
}

// Handle termination
process.on('SIGINT', async () => {
  console.log(colors.yellow('\nReceived SIGINT. Cleaning up...'));
  await cleanup();
  process.exit(0);
});

// Start the application
startCommandLoop();
