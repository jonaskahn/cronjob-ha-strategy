# Testing Guide for Cronjob-HA-Strategy

This directory contains tests for the `cronjob-ha-strategy` project. The tests are organized into different categories
to ensure comprehensive testing coverage.

## Test Structure

- `tests/client/`: Tests for client implementations (Redis, RabbitMQ, etcd)
- `tests/integration/`: Integration tests that test multiple components together
- `tests/unit/`: Unit tests for individual functions and classes (add as needed)

## Testing Approaches

### Mock-based Tests

By default, the unit tests use mock implementations of external services to isolate components during testing.

### Real Service Tests

For more thorough testing, you can run integration tests against real services running in Docker containers. This
approach tests the actual interaction with the services rather than mocked behaviors.

The test environment uses a simplified version of the services:

- Single Redis instance
- Single etcd instance
- Single RabbitMQ instance

This setup provides a lightweight environment for testing while still allowing you to verify service interactions.

## Running Tests

### Run all tests with mocks

```bash
npm test
```

### Run integration tests with real services

1. **Start the required services**:

```bash
npm run test:services:up
```

This will start Docker containers with one instance of each service type and initialize them for testing.

2. **Run the integration tests**:

```bash
npm run test:integration
```

3. **Check service logs if there are issues**:

```bash
npm run test:services:logs
```

4. **Restart services if needed**:

```bash
npm run test:services:restart
```

5. **Stop the services when done**:

```bash
npm run test:services:down
```

### Other testing commands

```bash
# Run tests in watch mode (during development)
npm run test:watch

# Run tests with coverage report
npm run test:coverage
```

## Writing Tests

### Unit Tests with Mocks

Unit tests should test individual functions or classes in isolation, using mocks for external dependencies.

```javascript
// Example unit test for a utility function
import { describe, test, expect, vi } from 'vitest';
import { myFunction } from '../../src/utils/myUtils.js';

describe('myFunction', () => {
  test('should return correct result', () => {
    expect(myFunction(1, 2)).toBe(3);
  });
});
```

### Integration Tests with Real Services

Integration tests should test how components work with actual external services.

```javascript
// Example integration test with real services
import { describe, test, expect } from 'vitest';
import redisClient from '../../src/client/redisClient.js';
import { v4 as uuidv4 } from 'uuid';

describe('Integration with Real Redis', () => {
  test('should store and retrieve data', async () => {
    // Generate unique test key
    const testKey = `test:${uuidv4()}`;

    // Perform real operations against the service
    await redisClient.set(testKey, 'test-value');
    const result = await redisClient.get(testKey);

    // Clean up
    await redisClient.del(testKey);

    // Verify results
    expect(result).toBe('test-value');
  });
});
```

## Testing Best Practices

1. **Test in isolation for unit tests**: Mock dependencies to isolate the component you're testing
2. **Use real services for integration tests**: Verify actual behavior with real services
3. **Use unique test identifiers**: When testing with real services, create unique keys for each test to avoid conflicts
4. **Clean up after tests**: Always delete test data created during tests to leave the environment clean
5. **Keep tests focused**: Each test should verify a single behavior
6. **Use descriptive test names**: Test names should clearly describe what is being tested

## Troubleshooting

If you encounter issues with the integration tests:

1. Verify the Docker services are running: `docker ps`
2. Check service logs: `npm run test:services:logs`
3. Try restarting the services: `npm run test:services:restart`
4. Check if Redis is responding: `redis-cli -h localhost -p 6379 ping`
5. Verify your `.env.test` file has the correct connection details
