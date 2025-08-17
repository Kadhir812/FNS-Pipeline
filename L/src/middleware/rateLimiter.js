import rateLimit from 'express-rate-limit';
import { config } from '../config/config.js';

// Create rate limiter
export const rateLimiter = rateLimit({
  windowMs: config.rateLimit.windowMs,
  max: config.rateLimit.max,
  message: {
    success: false,
    message: 'Too many requests from this IP, please try again later.',
    retryAfter: Math.ceil(config.rateLimit.windowMs / 1000)
  },
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({
      success: false,
      message: 'Too many requests from this IP, please try again later.',
      retryAfter: Math.ceil(config.rateLimit.windowMs / 1000)
    });
  }
});

// Stricter rate limit for search endpoints
export const searchRateLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 30, // 30 requests per minute for search
  message: {
    success: false,
    message: 'Too many search requests, please slow down.',
    retryAfter: 60
  },
  standardHeaders: true,
  legacyHeaders: false
});

export default { rateLimiter, searchRateLimiter };
