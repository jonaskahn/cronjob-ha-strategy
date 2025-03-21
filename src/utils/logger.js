import winston from 'winston';

// Console transport format
const consoleFormat = winston.format.combine(
  winston.format.colorize(),
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.printf(({ level, message, timestamp, ...meta }) => {
    const component = formatComponentWithLimit(meta);
    return `${timestamp} [${level}]:${component}: ${message}`;
  })
);

const formatComponentWithLimit = (meta, maxLength = 30) => {
  if (!meta?.component) return ''.padEnd(maxLength);
  const componentStr = `[${meta.component}]`;
  if (componentStr.length <= maxLength) {
    return componentStr.padEnd(maxLength);
  }
  return componentStr.slice(0, maxLength);
};

// Create logger instance
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'debug',
  format: consoleFormat,
  transports: [new winston.transports.Console()],
  exceptionHandlers: [new winston.transports.Console()],
  rejectionHandlers: [new winston.transports.Console()],
  exitOnError: false,
});

const getLoggerInstance = component => {
  return logger.child({ component });
};

export default getLoggerInstance;
