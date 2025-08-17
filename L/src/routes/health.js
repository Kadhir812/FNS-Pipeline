import express from 'express';
import healthController from '../controllers/healthController.js';

const router = express.Router();

// Health check endpoint
router.get('/health', healthController.healthCheck);

// Simple ping endpoint
router.get('/ping', healthController.ping);

export default router;
