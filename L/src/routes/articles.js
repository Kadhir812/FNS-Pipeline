import express from 'express';
import articleController from '../controllers/articleController.js';
import metricsController from '../controllers/metricsController.js';
// import { searchRateLimiter } from '../middleware/rateLimiter.js'; // Disabled for development
import { 
  validateSearchQuery, 
  validateArticleId, 
  validateSimilarArticles,
  validateHistoryQuery
} from '../middleware/validation.js';

const router = express.Router();

// Search articles
router.get('/search', 
  // searchRateLimiter, // Disabled for development
  metricsController.trackProcessing('backend'),
  validateSearchQuery,
  articleController.searchArticles
);

// Time-series history for a symbol
router.get('/history',
  metricsController.trackProcessing('backend'),
  validateHistoryQuery,
  articleController.getHistory
);

// Get article by ID
router.get('/:id',
  validateArticleId,
  articleController.getArticleById
);

// Get similar articles
router.get('/:id/similar',
  validateSimilarArticles,
  articleController.getSimilarArticles
);

// Get aggregations for filters
router.get('/meta/aggregations',
  articleController.getAggregations
);

export default router;
