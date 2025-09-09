import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { format } from 'date-fns';
import { getCategoryName } from '../../utils/articleUtils';

const CategoryStackedChart = ({ articles, height = 180 }) => {
  // Process articles data for the stacked area chart
  const processChartData = () => {
    if (!articles || articles.length === 0) return [];

    console.log('Processing chart data with articles:', articles.length);
    if (articles.length > 0) {
      console.log('First article category processing:', articles[0].category, '->', getCategoryName(articles[0].category || 'J'));
    }
    
    // Sort articles by publishedAt
    const sortedArticles = [...articles].sort((a, b) => {
      const dateA = new Date(a.publishedAt || a.published_date || a.timestamp || Date.now());
      const dateB = new Date(b.publishedAt || b.published_date || b.timestamp || Date.now());
      return dateA - dateB;
    });

    // Group articles by hour and category
    const hourlyData = {};
    
    sortedArticles.forEach((article, index) => {
      const dateValue = article.publishedAt || article.published_date || article.timestamp;
      let date;
      
      // Handle different date formats
      if (typeof dateValue === 'number') {
        // Elasticsearch timestamp (milliseconds)
        date = new Date(dateValue);
      } else if (typeof dateValue === 'string') {
        date = new Date(dateValue);
      } else {
        date = new Date();
      }
      
      // Skip invalid dates
      if (isNaN(date.getTime())) {
        console.warn('Invalid date for article:', article, 'dateValue:', dateValue);
        return;
      }
      
      const hourKey = format(date, 'yyyy-MM-dd HH:00');
      const category = getCategoryName(article.category || 'O');
      
      if (index < 3) {
        console.log(`Article ${index}: category="${article.category}" -> "${category}", date="${dateValue}" -> ${date.toISOString()}, hourKey="${hourKey}"`);
      }
      
      if (!hourlyData[hourKey]) {
        hourlyData[hourKey] = {
          time: date.getTime(),
          timeLabel: format(date, 'MMM dd HH:mm'),
          categories: {}
        };
      }
      
      if (!hourlyData[hourKey].categories[category]) {
        hourlyData[hourKey].categories[category] = 0;
      }
      
      hourlyData[hourKey].categories[category]++;
    });

    // Convert to array format suitable for recharts
    const chartArray = Object.values(hourlyData)
      .map(hour => ({
        time: hour.time,
        timeLabel: hour.timeLabel,
        ...hour.categories
      }))
      .sort((a, b) => a.time - b.time)
      .slice(-12); // Show last 12 hours of data

    // Ensure all categories have values for all time slots (fill with 0 if missing)
    const allCategories = new Set();
    articles?.forEach(article => {
      allCategories.add(getCategoryName(article.category || 'O'));
    });

    return chartArray.map(timeSlot => {
      const normalizedSlot = { ...timeSlot };
      Array.from(allCategories).forEach(category => {
        if (!(category in normalizedSlot)) {
          normalizedSlot[category] = 0;
        }
      });
      return normalizedSlot;
    });
  };

  // Get all unique categories from the data
  const getAllCategories = () => {
    const categories = new Set();
    articles?.forEach(article => {
      categories.add(getCategoryName(article.category || 'O'));
    });
    return Array.from(categories);
  };

  const chartData = processChartData();
  const categories = getAllCategories();

  // Enhanced sample data for testing (only when no articles)
  const getSampleData = () => {
    if (articles && articles.length > 0) {
      return []; // Return empty if we have real articles
    }
    
    const now = new Date();
    const sampleData = [];
    
    for (let i = 11; i >= 0; i--) {
      const time = new Date(now.getTime() - i * 60 * 60 * 1000); // Hours ago
      sampleData.push({
        time: time.getTime(),
        timeLabel: format(time, 'MMM dd HH:mm'),
        'Earnings': Math.floor(Math.random() * 10) + 2,
        'Mergers, Acquisitions & Deals': Math.floor(Math.random() * 8) + 1,
        'Economic Data & Federal Reserve': Math.floor(Math.random() * 6) + 1,
        'IPO & New Listings': Math.floor(Math.random() * 4) + 1,
        'Corporate Actions & Leadership': Math.floor(Math.random() * 5) + 1,
        'General Business & Industry News': Math.floor(Math.random() * 3) + 1
      });
    }
    
    return sampleData;
  };

  // Prioritize real data over sample data
  const sampleData = getSampleData();
  const finalChartData = chartData.length > 0 ? chartData : sampleData;
  const finalCategories = categories.length > 0 ? categories : ['Earnings', 'Mergers, Acquisitions & Deals', 'Economic Data & Federal Reserve', 'IPO & New Listings', 'Corporate Actions & Leadership', 'General Business & Industry News'];
  const isUsingSampleData = chartData.length === 0 && sampleData.length > 0;

  console.log('CategoryStackedChart - Articles:', articles?.length || 0);
  if (articles?.length > 0) {
    console.log('CategoryStackedChart - Sample Article Categories:', articles.slice(0, 3).map(a => ({ category: a.category, mapped: getCategoryName(a.category || 'J') })));
  }
  console.log('CategoryStackedChart - Final Chart Data Length:', finalChartData.length);
  console.log('CategoryStackedChart - Final Categories:', finalCategories);
  console.log('CategoryStackedChart - Using Sample Data:', isUsingSampleData);

  // Color palette for different categories (updated for real category names)
  const categoryColors = {
    'Earnings': '#3b82f6',                          // Blue
    'Analyst Ratings & Recommendations': '#10b981', // Green  
    'Mergers, Acquisitions & Deals': '#f59e0b',    // Orange
    'Regulatory & Legal Developments': '#ef4444',   // Red
    'Economic Data & Federal Reserve': '#8b5cf6',   // Purple
    'Corporate Actions & Leadership': '#ec4899',    // Pink
    'Market Trends & Sector Analysis': '#06b6d4',   // Cyan
    'IPO & New Listings': '#84cc16',                // Lime
    'Product Launches & Innovation': '#f97316',     // Orange-red
    'General Business & Industry News': '#6b7280',  // Gray
    'General News': '#6b7280',                      // Gray (fallback)
    // Legacy fallbacks for sample data
    'General': '#6b7280',
    'M&A': '#f59e0b',
    'Economic': '#ef4444',
    'IPO': '#84cc16',
    'Corporate': '#ec4899',
    'Other': '#6b7280'
  };

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const total = payload.reduce((sum, entry) => sum + (entry.value || 0), 0);
      return (
        <div className="category-chart-tooltip">
          <p className="tooltip-time">{label}</p>
          <p className="tooltip-total">Total Articles: {total}</p>
          {payload.map((entry, index) => (
            <p key={index} className="tooltip-category">
              <span 
                className="tooltip-color" 
                style={{ backgroundColor: entry.color }}
              ></span>
              <span className="tooltip-label">{entry.dataKey}:</span>
              <span className="tooltip-value">{entry.value || 0}</span>
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  if (finalChartData.length === 0) {
    return (
      <div className="category-chart-empty">
        <p>No data available for category distribution</p>
      </div>
    );
  }

  return (
    <div className="category-stacked-chart">
      <div className="chart-header">
        <h4>News Categories Over Time</h4>
        <p className="chart-subtitle">Article distribution by category (last 12 hours) {isUsingSampleData ? '(Sample Data)' : ''}</p>
      </div>
      <ResponsiveContainer width="100%" height={height}>
        <AreaChart
          data={finalChartData}
          margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
          <XAxis 
            dataKey="timeLabel"
            tick={{ fontSize: 10, fill: 'var(--text-tertiary)' }}
            axisLine={{ stroke: 'var(--border)' }}
            tickLine={{ stroke: 'var(--border)' }}
            angle={-45}
            textAnchor="end"
            height={60}
          />
          <YAxis 
            tick={{ fontSize: 10, fill: 'var(--text-tertiary)' }}
            axisLine={{ stroke: 'var(--border)' }}
            tickLine={{ stroke: 'var(--border)' }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend 
            wrapperStyle={{ fontSize: '10px' }}
            iconSize={8}
          />
          {finalCategories.map((category, index) => (
            <Area
              key={category}
              type="monotone"
              dataKey={category}
              stackId="1"
              stroke={categoryColors[category] || '#6b7280'}
              fill={categoryColors[category] || '#6b7280'}
              fillOpacity={0.6}
            />
          ))}
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
};

export default CategoryStackedChart;
