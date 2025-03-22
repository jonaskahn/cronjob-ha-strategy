import winston from 'winston';

// CONSOLE OUTPUT FORMAT
const consoleFormat = winston.format.combine(
  winston.format.colorize(),
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.printf(({ level, message, timestamp, ...meta }) => {
    const name = getFormattedName(meta);
    return `${timestamp} [${level}]:${name}: ${message}`;
  })
);

const getFormattedName = (meta, maxLength = 30) => {
  if (!meta?.component) return ''.padEnd(maxLength);
  const componentStr = `[${meta.component}]`;
  if (componentStr.length <= maxLength) {
    return componentStr.padEnd(maxLength);
  }
  return componentStr.slice(0, maxLength);
};

const logger = winston.createLogger({
  level: process.env.CHJS_LOG_LEVEL || 'debug',
  format: consoleFormat,
  transports: [new winston.transports.Console()],
  exceptionHandlers: [new winston.transports.Console()],
  rejectionHandlers: [new winston.transports.Console()],
  exitOnError: false,
});

const getLogger = name => {
  return logger.child({ name });
};

export default getLogger;
