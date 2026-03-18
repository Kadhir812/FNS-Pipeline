import logger from './logger.js';

let redisClient = null;
let isRedisAvailable = false;
let initAttempted = false;

async function initRedis() {
  // ✅ Already connected — skip
  if (redisClient && isRedisAvailable) return;

  // ✅ Don't hammer Redis on every call if it already failed once
  if (initAttempted && !isRedisAvailable) return;

  initAttempted = true;

  try {
    const { createClient } = await import('redis');
    redisClient = createClient({
      url: process.env.REDIS_URL || 'redis://redis:6379'
    });

    redisClient.on('error', (err) => {
      logger.error('Redis client error', { error: err.message });
      isRedisAvailable = false;
      redisClient = null;      // ✅ force re-init on next call
      initAttempted = false;   // ✅ allow retry after transient failure
    });

    redisClient.on('reconnecting', () => {
      logger.info('Redis reconnecting...');
    });

    redisClient.on('ready', () => {
      isRedisAvailable = true;
      logger.info('Redis ready');
    });

    await redisClient.connect();
    isRedisAvailable = true;
    logger.info('✅ Connected to Redis');
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
    return await redisClient.get(key);
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
    logger.warn('Redis del failed', { key, error: err.message });
    return false;
  }
}

// ✅ Use SCAN instead of KEYS — non-blocking, safe in production
async function clearPattern(pattern) {
  try {
    await initRedis();
    if (!isRedisAvailable || !redisClient) return false;

    let cursor = 0;
    let totalDeleted = 0;

    do {
      const { cursor: nextCursor, keys } = await redisClient.scan(cursor, {
        MATCH: pattern,
        COUNT: 100
      });
      cursor = nextCursor;
      if (keys.length > 0) {
        await redisClient.del(keys);
        totalDeleted += keys.length;
      }
    } while (cursor !== 0);

    if (totalDeleted > 0) {
      logger.info(`Cleared ${totalDeleted} cache keys matching: ${pattern}`);
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
  isAvailable: () => isRedisAvailable,
  getClient: () => redisClient 
};