export default {
  transform: {},
  testEnvironment: 'node',
  verbose: true,
  collectCoverageFrom: ['src/**/*.js'],
  testMatch: ['**/tests/unit/**/*.test.js'],
  setupFiles: ['dotenv/config'],
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
};
