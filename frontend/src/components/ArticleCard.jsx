import React from 'react';
import { TrendingUp, TrendingDown, Minus, ExternalLink, Clock, Star, Tag, BarChart, Building } from 'lucide-react';
import { formatTimeAgo, getImpactBadge, getRiskLevel, getRiskLevelDisplay, getRiskLevelColor, getCategoryName } from '../utils/articleUtils';
import RiskGauge from './ui/RiskGauge';
import SentimentBar from './ui/SentimentBar';
import './ArticleCard.css';

const ArticleCard = ({ article, onReadMore }) => {
  // Handle different possible field names and fallbacks
  const title = article.title || article.headline || 'No Title';
  const summary = article.summary || article.description || article.content?.substring(0, 150) + '...' || 'No summary available';
  const source = article.source || 'Unknown Source';
  const publishedDate = article.published_date || article.publishedAt || article.timestamp || new Date().toISOString();
  const sentiment = parseFloat(article.sentiment) || 0;
  const riskScore = article.risk_raw || article.risk_score || 0;
  const confidence = article.confidence || 0.8; // Default confidence if not available
  const category = article.category || 'J';
  const impact = article.impact_assessment || getRiskLevel(riskScore);
  const riskLevel = article.risk_level || getRiskLevel(riskScore); // Use Transform.py risk level
  const categoryName = getCategoryName(category);
  
  const impactBadge = getImpactBadge(impact);
  
  const getImpactIcon = (impact) => {
    switch (impact?.toUpperCase()) {
      case 'POSITIVE':
      case 'LOW':
        return <TrendingUp size={12} />;
      case 'NEGATIVE':
      case 'HIGH':
        return <TrendingDown size={12} />;
      default:
        return <Minus size={12} />;
    }
  };

  return (
    <article className="article-card">
      <div className="article-header">
        <div className="article-header-content">
          <h2 className="article-title">{title}</h2>
          <div className="article-meta">
            <span className="article-source">{source}</span>
            <span className="article-time">
              <Clock size={12} />
              {formatTimeAgo(publishedDate)}
            </span>
            {/* Show real published date */}
            <span className="article-real-date">
              {publishedDate ? new Date(publishedDate).toLocaleString() : ''}
            </span>
          </div>
        </div>
        <div className="article-impact-corner">
          <div className="impact-badge" style={{ backgroundColor: impactBadge.color }}>
            {getImpactIcon(impact)}
            {impact || 'NEUTRAL'}
          </div>
        </div>
      </div>

      <p className="article-summary">{summary}</p>

      {/* Stock Symbol and Entity Row */}
      {article.symbol && (
        <div className="article-entity-row">
          <div className="article-stock-symbol">
            <Tag size={14} />
            <strong>{article.symbol}</strong>
          </div>
          {article.entity_name && (
            <div className="article-entity-name" title={article.entity_name}>
              <Building size={14} />
              {article.entity_name}
            </div>
          )}
        </div>
      )}

      <div className="article-metrics">
        <div className="metric-row">
          <div className="risk-level-badge">
            <span 
              className={`risk-level ${riskLevel?.replace('_', '-')}`}
              style={{ 
                backgroundColor: `${getRiskLevelColor(riskLevel)}20`,
                color: getRiskLevelColor(riskLevel),
                borderColor: `${getRiskLevelColor(riskLevel)}40`
              }}
            >
              {getRiskLevelDisplay(riskLevel)}
            </span>
          </div>
          <div className="confidence-score">
            <Star size={12} />
            {Math.round(confidence * 100)}%
          </div>
        </div>

        <div className="metric-row">
          <div className="risk-metric">
            <span>Risk</span>
            <RiskGauge score={riskScore} />
          </div>
          <div className="sentiment-metric">
            <span>Sentiment</span>
            <SentimentBar sentiment={sentiment} />
          </div>
        </div>
      </div>

      <div className="article-tags">
        <div className="category-container">
          <BarChart size={12} />
          <span className="category-tag" title={categoryName}>
            {category} - {categoryName}
          </span>
        </div>
        <div className="key-phrases-container">
          {article.key_phrases && article.key_phrases.split(',').slice(0, 3).map((phrase, idx) => (
            <span key={idx} className="key-phrase">
              {phrase.trim()}
            </span>
          ))}
        </div>
      </div>

      <div className="article-actions">
        <button 
          className="read-more-btn"
          onClick={() => onReadMore(article)}
        >
          Read Full Analysis →
        </button>
        {(article.link || article.url) && (
          <a 
            href={article.link || article.url} 
            target="_blank" 
            rel="noopener noreferrer"
            className="external-link"
            title="Open article in new tab"
            style={{ 
              display: 'inline-flex', 
              alignItems: 'center', 
              justifyContent: 'center',
              padding: '8px',
              borderRadius: '4px',
              backgroundColor: 'var(--bg-secondary)',
              border: '1px solid var(--border-color)',
              color: 'var(--text-primary)',
              textDecoration: 'none',
              cursor: 'pointer',
              transition: 'all 0.2s ease'
            }}
            onMouseEnter={(e) => {
              e.target.style.backgroundColor = 'var(--accent-color)';
              e.target.style.color = 'white';
            }}
            onMouseLeave={(e) => {
              e.target.style.backgroundColor = 'var(--bg-secondary)';
              e.target.style.color = 'var(--text-primary)';
            }}
          >
            <ExternalLink size={16} />
          </a>
        )}
      </div>
    </article>
  );
};

export default ArticleCard;
