import { query, param } from 'express-validator';

// Validation for search articles
export const validateSearchQuery = [
  query('search')
    .optional()
    .isString()
    .trim()
    .isLength({ max: 500 })
    .withMessage('Search query must be a string with max 500 characters'),
  
  query('sentiment')
    .optional()
    .isIn(['all', 'positive', 'negative', 'neutral'])
    .withMessage('Sentiment must be one of: all, positive, negative, neutral'),
  
  query('category')
    .optional()
    .isString()
    .trim()
    .withMessage('Category must be a string'),
  
  query('source')
    .optional()
    .isString()
    .trim()
    .withMessage('Source must be a string'),
  
  query('riskScore')
    .optional()
    .custom((value) => {
      try {
        const parsed = JSON.parse(value);
        if (!Array.isArray(parsed) || parsed.length !== 2) {
          throw new Error('Risk score must be an array of 2 numbers');
        }
        if (parsed[0] < 0 || parsed[1] > 1 || parsed[0] > parsed[1]) {
          throw new Error('Risk score values must be between 0-1 and min <= max');
        }
        return true;
      } catch (error) {
        throw new Error('Risk score must be a valid JSON array of 2 numbers');
      }
    }),
  
  query('confidence')
    .optional()
    .custom((value) => {
      try {
        const parsed = JSON.parse(value);
        if (!Array.isArray(parsed) || parsed.length !== 2) {
          throw new Error('Confidence must be an array of 2 numbers');
        }
        if (parsed[0] < 0 || parsed[1] > 100 || parsed[0] > parsed[1]) {
          throw new Error('Confidence values must be between 0-100 and min <= max');
        }
        return true;
      } catch (error) {
        throw new Error('Confidence must be a valid JSON array of 2 numbers');
      }
    }),
  
  query('dateStart')
    .optional()
    .isISO8601()
    .withMessage('Start date must be a valid ISO 8601 date'),
  
  query('dateEnd')
    .optional()
    .isISO8601()
    .withMessage('End date must be a valid ISO 8601 date'),
  
  query('sortBy')
    .optional()
    .isIn(['newest', 'oldest', 'risk', 'sentiment', 'confidence'])
    .withMessage('Sort by must be one of: newest, oldest, risk, sentiment, confidence'),
  
  query('page')
    .optional()
    .isInt({ min: 1, max: 1000 })
    .withMessage('Page must be an integer between 1 and 1000'),
  
  query('limit')
    .optional()
    .isInt({ min: 1, max: 100 })
    .withMessage('Limit must be an integer between 1 and 100')
];

// Validation for article ID parameter
export const validateArticleId = [
  param('id')
    .notEmpty()
    .isString()
    .trim()
    .isLength({ min: 1, max: 255 })
    .withMessage('Article ID must be a non-empty string with max 255 characters')
];

// Validation for similar articles
export const validateSimilarArticles = [
  ...validateArticleId,
  query('limit')
    .optional()
    .isInt({ min: 1, max: 20 })
    .withMessage('Limit must be an integer between 1 and 20')
];

export default {
  validateSearchQuery,
  validateArticleId,
  validateSimilarArticles
};
