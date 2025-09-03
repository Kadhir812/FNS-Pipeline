import React from 'react';
import { Tag, Building, BarChart } from 'lucide-react';
import { formatTimeAgo, getCategoryName } from '../utils/articleUtils';

const ArticleModal = ({ article, onClose }) => {
  if (!article) return null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>{article.title}</h2>
          <button 
            className="modal-close"
            onClick={onClose}
          >
            ×
          </button>
        </div>
        <div className="modal-body">
          <div className="modal-meta">
            <span>{article.source}</span>
            <span>{formatTimeAgo(article.publishedAt)}</span>
          </div>
          
          {/* Stock and Entity Information */}
          {article.symbol && (
            <div className="modal-entity-info">
              <div className="entity-row">
                <div className="entity-item">
                  <Tag size={16} />
                  <strong>Symbol:</strong> {article.symbol}
                </div>
                {article.entity_name && (
                  <div className="entity-item">
                    <Building size={16} />
                    <strong>Company:</strong> {article.entity_name}
                  </div>
                )}
              </div>
            </div>
          )}
          <div className="modal-metrics">
            <div className="modal-metric">
              <strong>Impact:</strong> {article.impact_assessment}
            </div>
            <div className="modal-metric">
              <strong>Risk Score:</strong> {((article.risk_raw || article.risk_score || 0) * 100).toFixed(0)}%
            </div>
            <div className="modal-metric">
              <strong>Confidence:</strong> {((article.confidence || 0) * 100).toFixed(0)}%
            </div>
            <div className="modal-metric">
              <strong>Sentiment:</strong> {(parseFloat(article.sentiment || 0) * 100).toFixed(0)}%
            </div>
            <div className="modal-metric">
              <strong>Category:</strong> <BarChart size={14} style={{verticalAlign: 'middle'}} /> {article.category} - {getCategoryName(article.category)}
            </div>
          </div>
          <div className="modal-summary">
            <h4>Summary</h4>
            <p>{article.summary}</p>
          </div>
          <div className="modal-content-text">
            <h4>Full Content</h4>
            <p>{article.content}</p>
          </div>
          <div className="modal-keyphrases">
            <h4>Key Phrases</h4>
            <div className="keyphrases-list">
              {article.key_phrases && article.key_phrases.split(',').map((phrase, idx) => (
                <span key={idx} className="keyphrase-tag">
                  {phrase.trim()}
                </span>
              ))}
            </div>
          </div>
          <div className="modal-actions">
            <a 
              href={article.link} 
              target="_blank" 
              rel="noopener noreferrer"
              className="external-link-btn"
            >
              Read Original Article
            </a>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ArticleModal;
