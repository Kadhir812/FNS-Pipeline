import React, { useState, useEffect } from 'react';
import Header from '../components/Header';
import Sidebar from '../components/Sidebar';
import SortControls from '../components/SortControls';
import ArticleCard from '../components/ArticleCard';
import ArticleModal from '../components/ArticleModal';
import LoadingSpinner from '../components/ui/LoadingSpinner';
import { useArticles } from '../hooks/useArticles';
import { useFilters } from '../hooks/useFilters';

const Dashboard = () => {
  const { filters, updateFilter, getAPIFilters, availableFilters } = useFilters();
  const { 
    articles, 
    loading, 
    error, 
    totalResults, 
    currentPage, 
    totalPages,
    searchArticles,
    loadPage 
  } = useArticles();
  const [selectedArticle, setSelectedArticle] = useState(null);

  // Extract unique tickers from articles
  const tickerSet = new Set();
  const tickers = articles
    .filter(a => a.symbol && a.entity_name)
    .map(a => {
      const key = `${a.symbol}|${a.entity_name}`;
      if (!tickerSet.has(key)) {
        tickerSet.add(key);
        return { symbol: a.symbol, entity_name: a.entity_name };
      }
      return null;
    })
    .filter(Boolean);

  // Debug: Log ticker information
  console.log('📊 Total articles:', articles.length);
  console.log('📊 Articles with symbol:', articles.filter(a => a.symbol).length);
  console.log('📊 Articles with entity_name:', articles.filter(a => a.entity_name).length);
  console.log('📊 Articles with both symbol and entity_name:', articles.filter(a => a.symbol && a.entity_name).length);
  console.log('📊 Sample articles with symbols:', articles.filter(a => a.symbol && a.entity_name).slice(0, 3).map(a => ({ symbol: a.symbol, entity_name: a.entity_name, title: a.title.substring(0, 50) + '...' })));
  console.log('📊 Extracted tickers:', tickers);
  
  // Debug: Log unique impact_assessment values
  console.log('Unique impact_assessment values:', Array.from(new Set(articles.map(a => a.impact_assessment))));

  // Handle filter changes
  const handleFilterChange = (key, value) => {
    if (key === 'reset') {
      console.log('🔄 Resetting filters');
      // Reset all filters manually to ensure proper state
      updateFilter('search', '');
      updateFilter('sentiment', 'all');
      updateFilter('riskLevel', 'all');
      updateFilter('category', 'all');
      updateFilter('source', 'all');
      updateFilter('dateRange', { start: '', end: '' });
      updateFilter('sortBy', 'date');
      updateFilter('sortOrder', 'desc');
    } else {
      console.log('🎛️ Dashboard filter change:', key, value);
      updateFilter(key, value);
    }
  };

  // Handle sort changes
  const handleSortChange = (sortValue) => {
    const [sortBy, sortOrder] = sortValue.split('-');
    updateFilter('sortBy', sortBy);
    updateFilter('sortOrder', sortOrder || 'desc');
  };

  // Search articles when filters change - but debounce it
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      const apiFilters = getAPIFilters();
      console.log('🔄 Filter changed, searching with:', apiFilters);
      searchArticles(apiFilters);
    }, 300); // Reduced debounce time for better UX

    return () => clearTimeout(timeoutId);
  }, [filters, searchArticles, getAPIFilters]); // Proper dependencies

  const handleReadMore = (article) => {
    setSelectedArticle(article);
  };

  const handleCloseModal = () => {
    setSelectedArticle(null);
  };

  const handlePageChange = (page) => {
    loadPage(page);
  };

  // Filter articles by selected ticker, category, sentiment, and risk level
  const filteredArticles = articles.filter(article => {
    const tickerMatch = !filters.ticker || filters.ticker === 'all' || article.symbol === filters.ticker;
    const categoryMatch = !filters.category || filters.category === 'all' || article.category === filters.category;
    const sentimentMatch = !filters.sentiment || filters.sentiment === 'all' || article.impact_assessment === filters.sentiment;
    const riskLevelMatch = !filters.riskLevel || filters.riskLevel === 'all' || article.risk_level === filters.riskLevel;
    return tickerMatch && categoryMatch && sentimentMatch && riskLevelMatch;
  });

  if (loading) {
    return <LoadingSpinner message="Loading financial intelligence..." />;
  }

  if (error) {
    return (
      <div className="error-container">
        <h2>Error Loading Data</h2>
        <p>{error}</p>
        <button onClick={() => window.location.reload()}>
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="app">
      <Header insightCount={totalResults} />
      
      <div className="main-container">
        <Sidebar 
          filters={filters} 
          onFilterChange={handleFilterChange}
          availableFilters={{
            ...availableFilters,
            tickers,
            articles
          }}
        />
        
        <main className="main-content">
          <SortControls 
            sortBy={`${filters.sortBy}-${filters.sortOrder}`}
            onSortChange={handleSortChange}
          />
          
          <div className="articles-grid">
            {filteredArticles.length === 0 ? (
              <div className="no-articles">
                <h3>No articles found</h3>
                <p>Try adjusting your filters or search criteria.</p>
              </div>
            ) : (
              filteredArticles.map(article => (
                <ArticleCard
                  key={article._id || article.doc_id || article.id}
                  article={article}
                  onReadMore={handleReadMore}
                />
              ))
            )}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="pagination">
              <button 
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage === 1}
                className="pagination-btn"
              >
                Previous
              </button>
              
              <span className="pagination-info">
                Page {currentPage} of {totalPages} ({totalResults} total results)
              </span>
              
              <button 
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage === totalPages}
                className="pagination-btn"
              >
                Next
              </button>
            </div>
          )}
        </main>
      </div>

      <ArticleModal 
        article={selectedArticle}
        onClose={handleCloseModal}
      />
    </div>
  );
};

export default Dashboard;
