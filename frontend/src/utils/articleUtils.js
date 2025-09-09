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
  if (riskScore < 0.33) return 'low_risk';
  if (riskScore < 0.67) return 'medium_risk';
  return 'high_risk';
};

export const getRiskLevelDisplay = (riskLevel) => {
  switch (riskLevel) {
    case 'high_risk':
      return 'HIGH RISK';
    case 'moderate_risk':
      return 'MODERATE RISK';
    case 'medium_risk':
      return 'MEDIUM RISK';
    case 'low_risk':
      return 'LOW RISK';
    default:
      return 'LOW RISK';
  }
};

export const getRiskLevelColor = (riskLevel) => {
  switch (riskLevel) {
    case 'high_risk':
      return '#dc2626'; // red
    case 'moderate_risk':
      return '#f59e0b'; // orange
    case 'medium_risk':
      return '#0369a1'; // blue
    case 'low_risk':
      return '#16a34a'; // green
    default:
      return '#16a34a'; // green (default to low risk)
  }
};

export const getImpactBadge = (impact) => {
  switch (impact?.toUpperCase()) {
    case 'POSITIVE':
    case 'SLIGHTLY POSITIVE':
    case 'BULLISH':
      return { color: '#10b981', text: impact }; // green
    
    case 'NEGATIVE':
    case 'SLIGHTLY NEGATIVE':
    case 'BEARISH':
      return { color: '#ef4444', text: impact }; // red
    
    case 'HIGH RISK':
    case 'HIGH_RISK':
      return { color: '#dc2626', text: 'HIGH RISK' }; // dark red
    
    case 'MODERATE RISK':
    case 'MODERATE_RISK':
      return { color: '#f59e0b', text: 'MODERATE RISK' }; // orange
    
    case 'MEDIUM RISK':
    case 'MEDIUM_RISK':
      return { color: '#0369a1', text: 'MEDIUM RISK' }; // blue
    
    case 'LOW RISK':
    case 'LOW_RISK':
      return { color: '#16a34a', text: 'LOW RISK' }; // green
    
    case 'SPECULATIVE':
      return { color: '#8b5cf6', text: impact }; // purple
    
    case 'NEUTRAL':
    default:
      return { color: '#6b7280', text: impact || 'NEUTRAL' }; // gray
  }
};

// Map category codes to their full names
export const getCategoryName = (categoryCode) => {
  const categories = {
    'A': 'Earnings',
    'B': 'Analyst Ratings & Recommendations',
    'C': 'Mergers, Acquisitions & Deals',
    'D': 'Regulatory & Legal Developments',
    'E': 'Economic Data & Federal Reserve',
    'F': 'Corporate Actions & Leadership',
    'G': 'Market Trends & Sector Analysis',
    'H': 'IPO & New Listings',
    'I': 'Product Launches & Innovation',
    'J': 'General Business & Industry News'
  };
  
  return categories[categoryCode] || 'General News';
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
    const confidence = (article.conf_norm || (article.confidence && article.confidence <= 1 ? article.confidence : (article.confidence / 100)) || 0.8) * 100;
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
      return [...articles].sort((a, b) => {
        const confA = a.conf_norm || (a.confidence && a.confidence <= 1 ? a.confidence : (a.confidence / 100)) || 0.8;
        const confB = b.conf_norm || (b.confidence && b.confidence <= 1 ? b.confidence : (b.confidence / 100)) || 0.8;
        return confB - confA;
      });
    default:
      return articles;
  }
};
