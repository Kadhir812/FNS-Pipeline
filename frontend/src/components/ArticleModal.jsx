import React from 'react';
import { formatTimeAgo } from '../utils/articleUtils';

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
          <div className="modal-metrics">
            <div className="modal-metric">
              <strong>Impact:</strong> {article.impact_assessment}
            </div>
            <div className="modal-metric">
              <strong>Risk Score:</strong> {(article.risk_score * 100).toFixed(0)}%
            </div>
            <div className="modal-metric">
              <strong>Confidence:</strong> {(article.confidence * 100).toFixed(0)}%
            </div>
            <div className="modal-metric">
              <strong>Sentiment:</strong> {(article.sentiment * 100).toFixed(0)}%
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
