import elasticsearchService from '../services/elasticsearchService.js';

class HealthController {
  // Health check endpoint
  async healthCheck(req, res) {
    try {
      const health = await elasticsearchService.healthCheck();
      
      const status = health.elasticsearch === 'green' || health.elasticsearch === 'yellow' ? 200 : 503;
      
      res.status(status).json({
        success: status === 200,
        service: 'SignalEdge API',
        version: '1.0.0',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        environment: process.env.NODE_ENV || 'development',
        elasticsearch: health
      });
    } catch (error) {
      console.error('Health check error:', error);
      res.status(503).json({
        success: false,
        service: 'SignalEdge API',
        version: '1.0.0',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        environment: process.env.NODE_ENV || 'development',
        error: error.message
      });
    }
  }

  // Simple ping endpoint
  async ping(req, res) {
    res.json({
      success: true,
      message: 'pong',
      timestamp: new Date().toISOString()
    });
  }
}

export default new HealthController();
