import React from 'react';
import { Search, Filter } from 'lucide-react';
import './Sidebar.css';

const Sidebar = ({ filters, onFilterChange, availableFilters = {} }) => {
  const handleFilterChange = (key, value) => {
    console.log('🎛️ Filter changed:', key, '=', value);
    onFilterChange(key, value);
  };

  const { sources = [], categories = [], tickers = [] } = availableFilters;

  // Get unique impact_assessment values from articles for dynamic sentiment filter
  const impactAssessments = Array.from(new Set((availableFilters.articles || []).map(a => a.impact_assessment).filter(Boolean)));

  console.log('📊 Available filters:', { sources: sources.length, categories: categories.length });

  return (
    <aside className="sidebar">
      <div className="filter-section">
        <h3><Filter size={16} /> Filters</h3>
        
        <div className="filter-group">
          <label>Search</label>
          <div className="search-input">
            <Search size={16} />
            <input
              type="text"
              placeholder="Search articles, tickers..."
              value={filters.search}
              onChange={(e) => handleFilterChange('search', e.target.value)}
            />
          </div>
        </div>

        <div className="filter-group">
          <label>Date Range</label>
          <input
            type="date"
            value={filters.dateRange.start}
            onChange={(e) => handleFilterChange('dateRange', {
              ...filters.dateRange, 
              start: e.target.value
            })}
          />
          <input
            type="date"
            value={filters.dateRange.end}
            onChange={(e) => handleFilterChange('dateRange', {
              ...filters.dateRange, 
              end: e.target.value
            })}
          />
        </div>

        <div className="filter-group">
          <label>Sentiment</label>
          <select 
            value={filters.sentiment}
            onChange={(e) => handleFilterChange('sentiment', e.target.value)}
          >
            <option value="all">All Sentiment</option>
            {impactAssessments.map(val => (
              <option key={val} value={val}>{val}</option>
            ))}
          </select>
        </div>

        <div className="filter-group">
          <label>Category</label>
          <select 
            value={filters.category}
            onChange={(e) => handleFilterChange('category', e.target.value)}
          >
            <option value="all">All Categories</option>
            {categories.map(category => (
              <option key={category} value={category}>{category}</option>
            ))}
          </select>
        </div>

        <div className="filter-group">
          <label>Source</label>
          <select 
            value={filters.source}
            onChange={(e) => handleFilterChange('source', e.target.value)}
          >
            <option value="all">All Sources</option>
            {sources.map(source => (
              <option key={source} value={source}>{source}</option>
            ))}
          </select>
        </div>

        <div className="filter-group">
          <label>Risk Level</label>
          <select
            value={filters.riskLevel}
            onChange={(e) => handleFilterChange('riskLevel', e.target.value)}
          >
            <option value="all">All Risk Levels</option>
            <option value="low">Low Risk</option>
            <option value="medium">Medium Risk</option>
            <option value="high">High Risk</option>
          </select>
        </div>

        <div className="filter-group">
          <label>Tickers</label>
          <select 
            value={filters.ticker || 'all'}
            onChange={(e) => handleFilterChange('ticker', e.target.value)}
          >
            <option value="all">All Tickers</option>
            {tickers.map(tickerObj => (
              <option key={tickerObj.symbol} value={tickerObj.symbol}>
                {tickerObj.symbol} - {tickerObj.entity_name}
              </option>
            ))}
          </select>
        </div>

        <div className="filter-group">
          <button 
            onClick={() => {
              console.log('🔄 Resetting all filters');
              onFilterChange('reset', null);
            }}
            className="reset-filters-btn"
          >
            Reset All Filters
          </button>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;
