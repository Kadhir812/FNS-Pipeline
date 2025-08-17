import { formatDistanceToNow, parseISO } from 'date-fns';

export const formatTimeAgo = (dateString) => {
  try {
    let date;
    // Handle both timestamp and ISO string formats
    if (typeof dateString === 'number') {
      date = new Date(dateString);
    } else if (typeof dateString === 'string') {
      date = parseISO(dateString);
    } else {
      return 'Unknown time';
    }
    
    return formatDistanceToNow(date, { addSuffix: true });
  } catch (error) {
    return 'Unknown time';
  }
};

export const getSentimentColor = (sentiment) => {
  if (sentiment > 0.1) return '#10b981'; // green
  if (sentiment < -0.1) return '#ef4444'; // red
  return '#6b7280'; // gray
};

export const getRiskLevel = (riskScore) => {
  if (riskScore < 0.33) return 'LOW';
  if (riskScore < 0.67) return 'MEDIUM';
  return 'HIGH';
};

export const getImpactBadge = (impact) => {
  switch (impact?.toUpperCase()) {
    case 'POSITIVE':
    case 'LOW':
      return { color: '#10b981', text: impact };
    case 'NEGATIVE':
    case 'HIGH':
      return { color: '#ef4444', text: impact };
    case 'MEDIUM':
      return { color: '#f59e0b', text: impact };
    default:
      return { color: '#6b7280', text: 'NEUTRAL' };
  }
};

// Legacy functions for backward compatibility (not needed with API)
export const filterArticles = (articles, filters) => {
  return articles.filter(article => {
    // Date filter
    if (filters.dateRange.start || filters.dateRange.end) {
      const articleDate = new Date(article.published_date || article.publishedAt);
      if (filters.dateRange.start && articleDate < new Date(filters.dateRange.start)) return false;
      if (filters.dateRange.end && articleDate > new Date(filters.dateRange.end)) return false;
    }

    // Sentiment filter
    if (filters.sentiment !== 'all') {
      if (filters.sentiment === 'positive' && article.sentiment <= 0.1) return false;
      if (filters.sentiment === 'negative' && article.sentiment >= -0.1) return false;
      if (filters.sentiment === 'neutral' && Math.abs(article.sentiment) > 0.1) return false;
    }

    // Risk score filter
    if (article.risk_score < filters.riskScore[0] || article.risk_score > filters.riskScore[1]) return false;

    // Category filter
    if (filters.category !== 'all' && article.category !== filters.category) return false;

    // Source filter
    if (filters.source !== 'all' && article.source !== filters.source) return false;

    // Confidence filter
    const confidence = (article.confidence || 0.8) * 100;
    if (confidence < filters.confidence[0] || confidence > filters.confidence[1]) return false;

    // Search filter
    if (filters.search) {
      const searchTerm = filters.search.toLowerCase();
      const searchableText = `${article.title} ${article.key_phrases} ${article.content}`.toLowerCase();
      if (!searchableText.includes(searchTerm)) return false;
    }

    return true;
  });
};

export const sortArticles = (articles, sortBy) => {
  switch (sortBy) {
    case 'newest':
      return [...articles].sort((a, b) => new Date(b.published_date || b.publishedAt) - new Date(a.published_date || a.publishedAt));
    case 'oldest':
      return [...articles].sort((a, b) => new Date(a.published_date || a.publishedAt) - new Date(b.published_date || b.publishedAt));
    case 'risk':
      return [...articles].sort((a, b) => (b.risk_score || 0) - (a.risk_score || 0));
    case 'sentiment':
      return [...articles].sort((a, b) => (b.sentiment || 0) - (a.sentiment || 0));
    case 'confidence':
      return [...articles].sort((a, b) => (b.confidence || 0.8) - (a.confidence || 0.8));
    default:
      return articles;
  }
};
