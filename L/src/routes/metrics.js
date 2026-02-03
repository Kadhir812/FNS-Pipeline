import express from 'express';
import metricsController from '../controllers/metricsController.js';

const router = express.Router();

// GET /api/v1/metrics/performance - Get pipeline performance metrics
router.get('/performance', metricsController.getPerformanceMetrics.bind(metricsController));

export default router;
