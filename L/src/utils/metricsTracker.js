import logger from './logger.js';
import redisCache from './cache.js'; // ✅ reuse existing client

class MetricsTracker {
  constructor() {
    this.inMemoryMetrics = {
      throughput: {},
      latency:    {}
    };
  }

  // ✅ Single helper — no duplicate client management
  get redis() {
    return redisCache.isAvailable() ? redisCache : null;
  }

  async recordProcessed(pipeline, count = 1) {
    const key = `metrics:processed:${pipeline}:${this.getCurrentMinute()}`;
    try {
      if (this.redis) {
        await redisCache.getClient().incrBy(key, count);
        await redisCache.getClient().expire(key, 3600);
      } else {
        this.inMemoryMetrics.throughput[pipeline] =
          (this.inMemoryMetrics.throughput[pipeline] || 0) + count;
      }
    } catch (err) {
      logger.error('recordProcessed error', { pipeline, error: err.message });
      this.inMemoryMetrics.throughput[pipeline] =
        (this.inMemoryMetrics.throughput[pipeline] || 0) + count;
    }
  }

  async recordLatency(pipeline, latencyMs) {
    const key = `metrics:latency:${pipeline}`;
    try {
      if (this.redis) {
        await redisCache.getClient().rPush(key, String(latencyMs));
        await redisCache.getClient().lTrim(key, -1000, -1);
        await redisCache.getClient().expire(key, 3600);
      } else {
        this._pushInMemoryLatency(pipeline, latencyMs);
      }
    } catch (err) {
      logger.error('recordLatency error', { pipeline, error: err.message });
      this._pushInMemoryLatency(pipeline, latencyMs);
    }
  }

  _pushInMemoryLatency(pipeline, latencyMs) {
    if (!this.inMemoryMetrics.latency[pipeline])
      this.inMemoryMetrics.latency[pipeline] = [];
    this.inMemoryMetrics.latency[pipeline].push(latencyMs);
    if (this.inMemoryMetrics.latency[pipeline].length > 1000)
      this.inMemoryMetrics.latency[pipeline].shift();
  }

  async getMetrics() {
    const pipelines = ['extract', 'transform', 'backend', 'baseline'];
    const result   = {};

    // ✅ Capture once — avoid minute-boundary drift
    const currentMinute = this.getCurrentMinute();

    for (const pipeline of pipelines) {
      let throughput = 0;
      try {
        if (this.redis) {
          const keys   = Array.from({ length: 5 }, (_, i) =>
            `metrics:processed:${pipeline}:${currentMinute - i}`
          );
          const values = await Promise.all(
            keys.map(k => redisCache.get(k))
          );
          throughput = values.reduce((sum, v) => sum + (parseInt(v) || 0), 0) / 5;
        } else {
          throughput = this.inMemoryMetrics.throughput[pipeline] || 0;
        }
      } catch (err) {
        logger.error('getMetrics throughput error', { pipeline, error: err.message });
        throughput = this.inMemoryMetrics.throughput[pipeline] || 0;
      }

      let latencies = [];
      try {
        if (this.redis) {
          const raw = await redisCache.getClient().lRange(
            `metrics:latency:${pipeline}`, 0, -1
          );
          latencies = raw.map(v => parseFloat(v)).filter(v => !isNaN(v));
        } else {
          latencies = this.inMemoryMetrics.latency[pipeline] || [];
        }
      } catch (err) {
        logger.error('getMetrics latency error', { pipeline, error: err.message });
        latencies = this.inMemoryMetrics.latency[pipeline] || [];
      }

      result[pipeline] = {
        throughput: Math.round(throughput * 100) / 100,
        latency: this._percentiles(latencies)
      };
    }

    return result;
  }

  _percentiles(arr) {
    if (!arr.length) return { p50: 0, p95: 0, p99: 0, avg: 0 };
    const sorted = [...arr].sort((a, b) => a - b);
    const p = (pct) => sorted[Math.max(0, Math.ceil(sorted.length * pct) - 1)];
    return {
      p50: Math.round(p(0.50)),
      p95: Math.round(p(0.95)),
      p99: Math.round(p(0.99)),
      avg: Math.round(arr.reduce((s, v) => s + v, 0) / arr.length)
    };
  }

  getCurrentMinute() {
    return Math.floor(Date.now() / 60000);
  }
}

export default new MetricsTracker();