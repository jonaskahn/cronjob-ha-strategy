import { afterAll, beforeEach, describe, expect, it } from '@jest/globals';
import redisClient from '../../src/client/redisClient.js';

describe('RedisClient Unit Tests', () => {
  const testKey = 'unit:test:key';
  const testValue = 'unit-test-value';

  beforeEach(async () => {
    await redisClient.del(testKey);
  });

  afterAll(async () => {
    await redisClient.disconnect();
  });

  it('should set and get a key', async () => {
    await redisClient.set(testKey, testValue);

    const value = await redisClient.get(testKey);
    expect(value).toBe(testValue);
  });

  it('should delete a key', async () => {
    await redisClient.set(testKey, testValue);

    const result = await redisClient.del(testKey);
    expect(result).toBe(1);

    const value = await redisClient.get(testKey);
    expect(value).toBeNull();
  });

  it('should check existence of a key', async () => {
    await redisClient.set(testKey, testValue);

    const exists = await redisClient.exists(testKey);
    expect(exists).toBe(1);

    await redisClient.del(testKey);

    const notExists = await redisClient.exists(testKey);
    expect(notExists).toBe(0);
  });
});
