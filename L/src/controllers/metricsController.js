import metricsTracker from '../utils/metricsTracker.js';
import logger from '../utils/logger.js';

class MetricsController {
  // Get performance metrics
  async getPerformanceMetrics(req, res) {
    try {
      const metrics = await metricsTracker.getMetrics();
      
      // Transform for easier charting
      const throughputData = Object.entries(metrics).map(([pipeline, data]) => ({
        pipeline,
        value: data.throughput,
        label: pipeline.charAt(0).toUpperCase() + pipeline.slice(1)
      }));

      const latencyData = Object.entries(metrics).map(([pipeline, data]) => ({
        pipeline,
        p50: data.latency.p50,
        p95: data.latency.p95,
        p99: data.latency.p99,
        avg: data.latency.avg,
        label: pipeline.charAt(0).toUpperCase() + pipeline.slice(1)
      }));

      res.json({
        success: true,
        data: {
          throughput: throughputData,
          latency: latencyData,
          raw: metrics
        },
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      logger.error('getPerformanceMetrics error:', { error: error.message });
      res.status(500).json({
        success: false,
        message: 'Failed to fetch performance metrics',
        error: process.env.NODE_ENV === 'development' ? error.message : undefined
      });
    }
  }

  // Middleware to track article processing
  trackProcessing(pipeline) {
    return async (req, res, next) => {
      const start = Date.now();
      
      res.on('finish', async () => {
        const latency = Date.now() - start;
        await metricsTracker.recordLatency(pipeline, latency);
        if (res.statusCode < 400) {
          await metricsTracker.recordProcessed(pipeline, 1);
        }
      });
      
      next();
    };
  }
}

export default new MetricsController();
