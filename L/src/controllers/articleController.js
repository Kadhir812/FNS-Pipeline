import elasticsearchService from '../services/elasticsearchService.js';
import { validationResult } from 'express-validator';

class ArticleController {
  // Search articles
  async searchArticles(req, res) {
    try {
      // Check for validation errors
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: 'Validation errors',
          errors: errors.array()
        });
      }

      // Map frontend parameters to Elasticsearch service format
      const query = {
        q: req.query.search || req.query.q || '',
        sentiment: req.query.sentiment === 'all' ? '' : req.query.sentiment || '',
        category: req.query.category === 'all' ? '' : req.query.category || '',
        source: req.query.source === 'all' ? '' : req.query.source || '',
        risk_level: req.query.riskLevel || req.query.risk_level || '',
        risk_score_min: req.query.risk_score_min || req.query.riskScoreMin,
        risk_score_max: req.query.risk_score_max || req.query.riskScoreMax,
        start_date: req.query.dateStart || req.query.start_date || '',
        end_date: req.query.dateEnd || req.query.end_date || '',
        sort_by: req.query.sortBy === 'newest' ? 'date' : 
                 req.query.sortBy === 'oldest' ? 'date' :
                 req.query.sortBy === 'relevant' ? 'relevance' :
                 req.query.sortBy || 'date',
        sort_order: req.query.sortBy === 'oldest' ? 'asc' : 'desc',
        page: parseInt(req.query.page) || 1,
        page_size: parseInt(req.query.limit) || 20
      };

      console.log('Search query params:', query); // Debug log
      const result = await elasticsearchService.searchArticles(query);

      res.json({
        success: true,
        data: result,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error('Search articles error:', error);
      res.status(500).json({
        success: false,
        message: 'Internal server error',
        error: process.env.NODE_ENV === 'development' ? error.message : undefined
      });
    }
  }

  // Get article by ID
  async getArticleById(req, res) {
    try {
      const { id } = req.params;
      
      if (!id) {
        return res.status(400).json({
          success: false,
          message: 'Article ID is required'
        });
      }

      const article = await elasticsearchService.getArticleById(id);
      
      if (!article) {
        return res.status(404).json({
          success: false,
          message: 'Article not found'
        });
      }

      res.json({
        success: true,
        data: article,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error('Get article error:', error);
      res.status(500).json({
        success: false,
        message: 'Internal server error',
        error: process.env.NODE_ENV === 'development' ? error.message : undefined
      });
    }
  }

  // Get similar articles
  async getSimilarArticles(req, res) {
    try {
      const { id } = req.params;
      const limit = parseInt(req.query.limit) || 5;

      if (!id) {
        return res.status(400).json({
          success: false,
          message: 'Article ID is required'
        });
      }

      const similarArticles = await elasticsearchService.getSimilarArticles(id, limit);

      res.json({
        success: true,
        data: similarArticles,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error('Get similar articles error:', error);
      res.status(500).json({
        success: false,
        message: 'Internal server error',
        error: process.env.NODE_ENV === 'development' ? error.message : undefined
      });
    }
  }

  // Get aggregations for filters
  async getAggregations(req, res) {
    try {
      const aggregations = await elasticsearchService.getAggregations();

      res.json({
        success: true,
        data: aggregations,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error('Get aggregations error:', error);
      res.status(500).json({
        success: false,
        message: 'Internal server error',
        error: process.env.NODE_ENV === 'development' ? error.message : undefined
      });
    }
  }
}

export default new ArticleController();
