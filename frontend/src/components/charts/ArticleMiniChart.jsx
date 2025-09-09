import React from 'react';
import { LineChart, Line, ResponsiveContainer, Tooltip } from 'recharts';

const ArticleMiniChart = ({ article, relatedArticles = [], height = 60 }) => {
  // Create a simple trend line based on related articles or sentiment history
  const processData = () => {
    const data = [];
    const baseScore = parseFloat(article.final_score) || 0;
    const sentiment = parseFloat(article.sentiment) || 0;
    const risk = parseFloat(article.risk_raw || article.risk_score) || 0;
    
    // If we have related articles (same symbol/entity), use them for trend
    if (relatedArticles && relatedArticles.length > 0) {
      const sortedRelated = [...relatedArticles]
        .filter(a => a.publishedAt || a.published_date || a.timestamp)
        .sort((a, b) => {
          const dateA = new Date(a.publishedAt || a.published_date || a.timestamp);
          const dateB = new Date(b.publishedAt || b.published_date || b.timestamp);
          return dateA - dateB;
        })
        .slice(-10); // Last 10 related articles

      return sortedRelated.map((relArticle, index) => ({
        index,
        score: parseFloat(relArticle.final_score) || 0,
        sentiment: parseFloat(relArticle.sentiment) || 0
      }));
    }
    
    // Generate synthetic trend data based on current metrics
    const trendPoints = 8;
    for (let i = 0; i < trendPoints; i++) {
      const variance = (Math.random() - 0.5) * 0.3; // Random variance
      const trend = sentiment > 0 ? 0.1 : -0.1; // Slight trend based on sentiment
      const score = baseScore + (i * trend) + variance;
      
      data.push({
        index: i,
        score: Math.max(-3, Math.min(3, score)), // Clamp between -3 and 3
        sentiment: sentiment
      });
    }
    
    return data;
  };

  const chartData = processData();
  
  const getLineColor = () => {
    const avgScore = chartData.reduce((sum, d) => sum + d.score, 0) / chartData.length;
    if (avgScore > 0.5) return 'var(--success-500)';
    if (avgScore < -0.5) return 'var(--danger-500)';
    return 'var(--warning-500)';
  };

  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      return (
        <div className="mini-chart-tooltip">
          <p>Score: {payload[0].value.toFixed(2)}</p>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="article-mini-chart">
      <ResponsiveContainer width="100%" height={height}>
        <LineChart data={chartData}>
          <Line 
            type="monotone" 
            dataKey="score" 
            stroke={getLineColor()}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 3 }}
          />
          <Tooltip content={<CustomTooltip />} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default ArticleMiniChart;
