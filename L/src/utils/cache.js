import logger from './logger.js';

let redisClient = null;
let isRedisAvailable = false;

// Lazy-load redis client so app still works if package isn't installed / Redis down
async function initRedis() {
  if (redisClient || isRedisAvailable) return;
  try {
    const { createClient } = await import('redis');
    redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
    redisClient.on('error', (err) => {
      logger.error('Redis client error', { error: err.message });
      isRedisAvailable = false;
    });
    await redisClient.connect();
    isRedisAvailable = true;
    logger.info('Connected to Redis');
  } catch (err) {
    logger.warn('Redis unavailable - continuing without cache', { error: err.message });
    redisClient = null;
    isRedisAvailable = false;
  }
}

async function get(key) {
  try {
    await initRedis();
    if (!isRedisAvailable || !redisClient) return null;
    const v = await redisClient.get(key);
    return v;
  } catch (err) {
    logger.warn('Redis get failed', { key, error: err.message });
    return null;
  }
}

async function set(key, value, ttlSeconds = 120) {
  try {
    await initRedis();
    if (!isRedisAvailable || !redisClient) return false;
    await redisClient.set(key, value, { EX: ttlSeconds });
    return true;
  } catch (err) {
    logger.warn('Redis set failed', { key, error: err.message });
    return false;
  }
}

async function del(key) {
  try {
    await initRedis();
    if (!isRedisAvailable || !redisClient) return false;
    await redisClient.del(key);
    return true;
  } catch (err) {
    logger.warn('Redis delete failed', { key, error: err.message });
    return false;
  }
}

async function clearPattern(pattern) {
  try {
    await initRedis();
    if (!isRedisAvailable || !redisClient) return false;
    const keys = await redisClient.keys(pattern);
    if (keys.length > 0) {
      await redisClient.del(keys);
      logger.info(`Cleared ${keys.length} cache keys matching pattern: ${pattern}`);
    }
    return true;
  } catch (err) {
    logger.warn('Redis clearPattern failed', { pattern, error: err.message });
    return false;
  }
}

export default {
  get,
  set,
  del,
  clearPattern,
  // expose a flag useful for diagnostics
  isAvailable: () => isRedisAvailable
};
