import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { format } from 'date-fns';

const MarketSentimentChart = ({ articles, height = 200 }) => {
  // Process articles data for the chart
  const processChartData = () => {
    if (!articles || articles.length === 0) return [];

    console.log('MarketSentimentChart - Processing articles:', articles.length);

    // Sort articles by publishedAt
    const sortedArticles = [...articles].sort((a, b) => {
      const dateA = new Date(a.publishedAt || a.published_date || a.timestamp || Date.now());
      const dateB = new Date(b.publishedAt || b.published_date || b.timestamp || Date.now());
      return dateA - dateB;
    });

    // Group articles by hour to reduce noise
    const hourlyData = {};
    
    sortedArticles.forEach((article, index) => {
      const dateValue = article.publishedAt || article.published_date || article.timestamp;
      let date;
      
      // Handle different date formats
      if (typeof dateValue === 'number') {
        date = new Date(dateValue);
      } else if (typeof dateValue === 'string') {
        date = new Date(dateValue);
      } else {
        date = new Date();
      }
      
      if (isNaN(date.getTime())) {
        console.warn('Invalid date for article:', article);
        return;
      }
      
      const hourKey = format(date, 'yyyy-MM-dd HH:00');
      
      if (!hourlyData[hourKey]) {
        hourlyData[hourKey] = {
          time: date.getTime(),
          timeLabel: format(date, 'MMM dd, HH:mm'),
          scores: [],
          sentiment: [],
          risk: []
        };
      }
      
      const finalScore = parseFloat(article.final_score) || 0;
      const sentiment = parseFloat(article.sentiment) || 0;
      const risk = parseFloat(article.risk_raw || article.risk_score) || 0;
      
      if (index < 3) {
        console.log(`MarketSentiment Article ${index}: finalScore=${finalScore}, sentiment=${sentiment}, risk=${risk}`);
      }
      
      hourlyData[hourKey].scores.push(finalScore);
      hourlyData[hourKey].sentiment.push(sentiment);
      hourlyData[hourKey].risk.push(risk);
    });

    // Calculate averages for each hour
    return Object.values(hourlyData)
      .map(hour => ({
        time: hour.time,
        timeLabel: hour.timeLabel,
        finalScore: hour.scores.reduce((sum, score) => sum + score, 0) / hour.scores.length,
        avgSentiment: hour.sentiment.reduce((sum, sent) => sum + sent, 0) / hour.sentiment.length,
        avgRisk: hour.risk.reduce((sum, r) => sum + r, 0) / hour.risk.length,
        articleCount: hour.scores.length
      }))
      .sort((a, b) => a.time - b.time)
      .slice(-24); // Show last 24 hours of data
  };

  const chartData = processChartData();

  // Enhanced sample data only when no real articles
  const getSampleData = () => {
    if (articles && articles.length > 0) {
      return []; // Return empty if we have real articles
    }
    
    const now = new Date();
    const sampleData = [];
    
    for (let i = 23; i >= 0; i--) {
      const time = new Date(now.getTime() - i * 60 * 60 * 1000); // Hours ago
      sampleData.push({
        time: time.getTime(),
        timeLabel: format(time, 'MMM dd, HH:mm'),
        finalScore: (Math.random() - 0.5) * 4, // Random score between -2 and 2
        avgSentiment: Math.random() * 2 - 1, // Random sentiment between -1 and 1
        avgRisk: Math.random() * 0.8 + 0.1, // Random risk between 0.1 and 0.9
        articleCount: Math.floor(Math.random() * 20) + 5
      });
    }
    
    return sampleData;
  };

  // Prioritize real data over sample data
  const sampleData = getSampleData();
  const finalChartData = chartData.length > 0 ? chartData : sampleData;
  const isUsingSampleData = chartData.length === 0 && sampleData.length > 0;

  console.log('MarketSentimentChart - Articles:', articles?.length || 0);
  console.log('MarketSentimentChart - Final Chart Data Length:', finalChartData.length);

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="market-sentiment-tooltip">
          <p className="tooltip-time">{data.timeLabel}</p>
          <p className="tooltip-score">
            <span className="tooltip-label">Final Score:</span>
            <span className="tooltip-value">{data.finalScore.toFixed(2)}</span>
          </p>
          <p className="tooltip-sentiment">
            <span className="tooltip-label">Avg Sentiment:</span>
            <span className="tooltip-value">{(data.avgSentiment * 100).toFixed(1)}%</span>
          </p>
          <p className="tooltip-risk">
            <span className="tooltip-label">Avg Risk:</span>
            <span className="tooltip-value">{(data.avgRisk * 100).toFixed(1)}%</span>
          </p>
          <p className="tooltip-count">
            <span className="tooltip-label">Articles:</span>
            <span className="tooltip-value">{data.articleCount}</span>
          </p>
        </div>
      );
    }
    return null;
  };

  if (finalChartData.length === 0) {
    return (
      <div className="market-sentiment-chart-empty">
        <p>No data available for sentiment chart</p>
      </div>
    );
  }

  return (
    <div className="market-sentiment-chart">
      <div className="chart-header">
        <h4>Market Sentiment Evolution</h4>
        <p className="chart-subtitle">Final score trend over time (last 24 hours) {isUsingSampleData ? '(Sample Data)' : ''}</p>
      </div>
      <ResponsiveContainer width="100%" height={height}>
        <LineChart
          data={finalChartData}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
          <XAxis 
            dataKey="timeLabel"
            tick={{ fontSize: 12, fill: 'var(--text-tertiary)' }}
            axisLine={{ stroke: 'var(--border)' }}
            tickLine={{ stroke: 'var(--border)' }}
            angle={-45}
            textAnchor="end"
            height={60}
          />
          <YAxis 
            tick={{ fontSize: 12, fill: 'var(--text-tertiary)' }}
            axisLine={{ stroke: 'var(--border)' }}
            tickLine={{ stroke: 'var(--border)' }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Line 
            type="monotone" 
            dataKey="finalScore" 
            stroke="var(--primary-500)" 
            strokeWidth={2}
            dot={{ fill: 'var(--primary-500)', strokeWidth: 2, r: 4 }}
            activeDot={{ r: 6, stroke: 'var(--primary-500)', strokeWidth: 2 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default MarketSentimentChart;
