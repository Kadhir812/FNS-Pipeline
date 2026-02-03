import logger from './logger.js';

class MetricsTracker {
  constructor() {
    this.redisClient = null;
    this.inMemoryMetrics = {
      throughput: {},
      latency: {}
    };
    this.initRedis();
  }

  async initRedis() {
    try {
      const { createClient } = await import('redis');
      this.redisClient = createClient({
        url: process.env.REDIS_URL || 'redis://localhost:6379'
      });
      
      this.redisClient.on('error', (err) => {
        logger.error('MetricsTracker Redis error:', { error: err.message });
        this.redisClient = null;
      });
      
      await this.redisClient.connect();
      logger.info('✅ MetricsTracker: Redis connected');
    } catch (err) {
      logger.warn('⚠️ MetricsTracker: Redis unavailable, using in-memory fallback', { error: err.message });
      this.redisClient = null;
    }
  }

  async recordProcessed(pipeline, count = 1) {
    const key = `metrics:processed:${pipeline}:${this.getCurrentMinute()}`;
    try {
      if (this.redisClient && this.redisClient.isOpen) {
        await this.redisClient.incrBy(key, count);
        await this.redisClient.expire(key, 3600); // 1 hour TTL
      } else {
        this.inMemoryMetrics.throughput[pipeline] = (this.inMemoryMetrics.throughput[pipeline] || 0) + count;
      }
    } catch (err) {
      logger.error('recordProcessed error:', { pipeline, error: err.message });
      this.inMemoryMetrics.throughput[pipeline] = (this.inMemoryMetrics.throughput[pipeline] || 0) + count;
    }
  }

  async recordLatency(pipeline, latencyMs) {
    const key = `metrics:latency:${pipeline}`;
    try {
      if (this.redisClient && this.redisClient.isOpen) {
        await this.redisClient.rPush(key, String(latencyMs));
        await this.redisClient.lTrim(key, -1000, -1); // keep last 1000
        await this.redisClient.expire(key, 3600);
      } else {
        if (!this.inMemoryMetrics.latency[pipeline]) this.inMemoryMetrics.latency[pipeline] = [];
        this.inMemoryMetrics.latency[pipeline].push(latencyMs);
        if (this.inMemoryMetrics.latency[pipeline].length > 1000) {
          this.inMemoryMetrics.latency[pipeline].shift();
        }
      }
    } catch (err) {
      logger.error('recordLatency error:', { pipeline, error: err.message });
      if (!this.inMemoryMetrics.latency[pipeline]) this.inMemoryMetrics.latency[pipeline] = [];
      this.inMemoryMetrics.latency[pipeline].push(latencyMs);
    }
  }

  async getMetrics() {
    const pipelines = ['extract', 'transform', 'backend', 'baseline'];
    const result = {};

    for (const pipeline of pipelines) {
      // Throughput (articles per minute in last 5 min)
      let throughput = 0;
      try {
        if (this.redisClient && this.redisClient.isOpen) {
          const keys = [];
          for (let i = 0; i < 5; i++) {
            keys.push(`metrics:processed:${pipeline}:${this.getCurrentMinute() - i}`);
          }
          const values = await Promise.all(keys.map(k => this.redisClient.get(k).catch(() => null)));
          throughput = values.reduce((sum, v) => sum + (parseInt(v) || 0), 0) / 5;
        } else {
          throughput = this.inMemoryMetrics.throughput[pipeline] || 0;
        }
      } catch (err) {
        logger.error('getMetrics throughput error:', { pipeline, error: err.message });
        throughput = this.inMemoryMetrics.throughput[pipeline] || 0;
      }

      // Latency (p50, p95, p99)
      let latencies = [];
      try {
        if (this.redisClient && this.redisClient.isOpen) {
          const raw = await this.redisClient.lRange(`metrics:latency:${pipeline}`, 0, -1);
          latencies = raw.map(v => parseFloat(v)).filter(v => !isNaN(v));
        } else {
          latencies = this.inMemoryMetrics.latency[pipeline] || [];
        }
      } catch (err) {
        logger.error('getMetrics latency error:', { pipeline, error: err.message });
        latencies = this.inMemoryMetrics.latency[pipeline] || [];
      }

      const percentile = (arr, p) => {
        if (!arr.length) return 0;
        const sorted = [...arr].sort((a, b) => a - b);
        const idx = Math.ceil(sorted.length * p) - 1;
        return sorted[Math.max(0, idx)];
      };

      result[pipeline] = {
        throughput: Math.round(throughput * 100) / 100,
        latency: {
          p50: Math.round(percentile(latencies, 0.5)),
          p95: Math.round(percentile(latencies, 0.95)),
          p99: Math.round(percentile(latencies, 0.99)),
          avg: latencies.length ? Math.round(latencies.reduce((s, v) => s + v, 0) / latencies.length) : 0
        }
      };
    }

    return result;
  }

  getCurrentMinute() {
    return Math.floor(Date.now() / 60000); // minute bucket
  }

  async close() {
    if (this.redisClient && this.redisClient.isOpen) {
      await this.redisClient.quit();
    }
  }
}

export default new MetricsTracker();
