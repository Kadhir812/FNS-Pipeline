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

  // Handle filter changes
  const handleFilterChange = (key, value) => {
    updateFilter(key, value);
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
      searchArticles(apiFilters);
    }, 500); // Increased debounce time

    return () => clearTimeout(timeoutId);
  }, [JSON.stringify(filters)]); // Use JSON.stringify to prevent object reference issues

  const handleReadMore = (article) => {
    setSelectedArticle(article);
  };

  const handleCloseModal = () => {
    setSelectedArticle(null);
  };

  const handlePageChange = (page) => {
    loadPage(page);
  };

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
          availableFilters={availableFilters}
        />
        
        <main className="main-content">
          <SortControls 
            sortBy={`${filters.sortBy}-${filters.sortOrder}`}
            onSortChange={handleSortChange}
          />
          
          <div className="articles-grid">
            {articles.length === 0 ? (
              <div className="no-articles">
                <h3>No articles found</h3>
                <p>Try adjusting your filters or search criteria.</p>
              </div>
            ) : (
              articles.map(article => (
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
