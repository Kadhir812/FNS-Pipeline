import React from 'react';
import { getSentimentColor } from '../../utils/articleUtils';

const SentimentBar = ({ sentiment }) => (
  <div className="sentiment-bar">
    <div 
      className="sentiment-fill"
      style={{ 
        width: `${Math.abs(sentiment) * 100}%`,
        backgroundColor: getSentimentColor(sentiment),
        marginLeft: sentiment < 0 ? `${(1 + sentiment) * 100}%` : '0'
      }}
    />
  </div>
);

export default SentimentBar;
