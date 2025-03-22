import { afterAll, beforeEach, describe, expect, it, jest } from '@jest/globals';
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

  it('should handle set errors gracefully', async () => {
    const originalSet = redisClient._client.set;
    redisClient._client.set = jest.fn().mockRejectedValue(new Error('Mock set error'));

    await expect(redisClient.set(testKey, testValue)).rejects.toThrow('Mock set error');

    redisClient._client.set = originalSet;
  });

  it('should handle get errors gracefully', async () => {
    const originalGet = redisClient._client.get;
    redisClient._client.get = jest.fn().mockRejectedValue(new Error('Mock get error'));

    await expect(redisClient.get(testKey)).rejects.toThrow('Mock get error');

    redisClient._client.get = originalGet;
  });

  it('should handle delete errors gracefully', async () => {
    const originalDel = redisClient._client.del;
    redisClient._client.del = jest.fn().mockRejectedValue(new Error('Mock del error'));

    await expect(redisClient.del(testKey)).rejects.toThrow('Mock del error');

    redisClient._client.del = originalDel;
  });

  it('should handle exists errors gracefully', async () => {
    const originalExists = redisClient._client.exists;
    redisClient._client.exists = jest.fn().mockRejectedValue(new Error('Mock exists error'));

    await expect(redisClient.exists(testKey)).rejects.toThrow('Mock exists error');

    redisClient._client.exists = originalExists;
  });
});
