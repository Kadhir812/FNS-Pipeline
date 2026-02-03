// Lightweight frontend API helpers
export async function fetchArticleHistory(symbol, limit = 200, from = null, to = null) {
  if (!symbol) return [];
  try {
    const params = new URLSearchParams();
    params.set('symbol', symbol);
    if (limit) params.set('limit', String(limit));
    if (from) params.set('from', String(from));
    if (to) params.set('to', String(to));

    const res = await fetch(`/api/v1/articles/history?${params.toString()}`, {
      method: 'GET',
      headers: { 'Accept': 'application/json' }
    });

    if (!res.ok) {
      // Try to parse error body for debugging
      let errText = await res.text().catch(() => '');
      console.warn('fetchArticleHistory failed', res.status, errText);
      return [];
    }

    const payload = await res.json();
    if (payload && payload.success && Array.isArray(payload.data)) return payload.data;
    return [];
  } catch (err) {
    console.warn('fetchArticleHistory error', err.message || err);
    return [];
  }
}

import axios from 'axios';

// Create axios instance with base configuration
const api = axios.create({
  baseURL: 'http://localhost:3001/api/v1',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for logging
api.interceptors.request.use(
  (config) => {
    console.log(`Making ${config.method?.toUpperCase()} request to ${config.url}`);
    return config;
  },
  (error) => {
    console.error('Request error:', error);
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => {
    return response.data;
  },
  (error) => {
    console.error('API Error:', error.response?.data || error.message);
    
    // Handle specific error cases
    if (error.response?.status === 429) {
      throw new Error('Too many requests. Please try again later.');
    } else if (error.response?.status === 503) {
      throw new Error('Service temporarily unavailable. Please try again later.');
    } else if (error.response?.status >= 500) {
      throw new Error('Server error. Please try again later.');
    } else if (error.response?.status === 404) {
      throw new Error('Resource not found.');
    }
    
    throw new Error(error.response?.data?.message || 'An error occurred while fetching data.');
  }
);

// API methods
export const articleAPI = {
  // Search articles with filters
  searchArticles: async (params = {}) => {
    const searchParams = new URLSearchParams();
    
    // Add search parameters with validation
    if (params.q?.trim()) searchParams.append('q', params.q.trim());
    if (params.source && params.source !== 'all') searchParams.append('source', params.source);
    if (params.category && params.category !== 'all') searchParams.append('category', params.category);
    if (params.sentiment && params.sentiment !== 'all') searchParams.append('sentiment', params.sentiment);
    if (params.risk_level && params.risk_level !== 'all') searchParams.append('risk_level', params.risk_level);
    if (params.risk_score_min !== undefined && params.risk_score_min !== '') searchParams.append('risk_score_min', params.risk_score_min);
    if (params.risk_score_max !== undefined && params.risk_score_max !== '') searchParams.append('risk_score_max', params.risk_score_max);
    if (params.start_date) searchParams.append('start_date', params.start_date);
    if (params.end_date) searchParams.append('end_date', params.end_date);
    if (params.page) searchParams.append('page', params.page);
    if (params.page_size) searchParams.append('page_size', params.page_size);
    if (params.sort_by) searchParams.append('sort_by', params.sort_by);
    if (params.sort_order) searchParams.append('sort_order', params.sort_order);
    
    const url = `/articles/search${searchParams.toString() ? '?' + searchParams.toString() : ''}`;
    console.log('🌐 API Request URL:', url);
    
    return api.get(url);
  },

  // Get specific article by ID
  getArticleById: async (id) => {
    return api.get(`/articles/${id}`);
  },

  // Get similar articles
  getSimilarArticles: async (id, limit = 5) => {
    return api.get(`/articles/${id}/similar?limit=${limit}`);
  },

  // Get filter metadata/aggregations
  getFilterMetadata: async () => {
    return api.get('/articles/meta/aggregations');
  },
};

// Health check API
export const healthAPI = {
  // Check API health
  checkHealth: async () => {
    return api.get('/health/health');
  },

  // Simple ping
  ping: async () => {
    return api.get('/health/ping');
  },
};

export default api;
