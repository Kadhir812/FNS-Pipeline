import { useState, useEffect, useCallback } from 'react';
import { articleAPI } from '../services/api';

export const useArticles = (initialFilters = {}) => {
  const [articles, setArticles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [totalResults, setTotalResults] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);
  const [filters, setFilters] = useState(initialFilters);

  const fetchArticles = useCallback(async (searchFilters = {}, page = 1) => {
    try {
      setLoading(true);
      setError(null);
      
      const searchParams = {
        ...searchFilters,
        page,
        page_size: 20, // Default page size
      };

      const response = await articleAPI.searchArticles(searchParams);
      
      if (response.success) {
        setArticles(response.data.articles || []);
        setTotalResults(response.data.total || 0);
        setCurrentPage(response.data.page || 1);
        setTotalPages(response.data.total_pages || 0);
      } else {
        setError('Failed to fetch articles');
      }
    } catch (err) {
      console.error('Error fetching articles:', err);
      setError(err.message || 'Failed to fetch articles');
      setArticles([]);
    } finally {
      setLoading(false);
    }
  }, []); // Remove filters dependency to prevent infinite loop

  // Search with new filters
  const searchArticles = useCallback((newFilters) => {
    fetchArticles(newFilters, 1);
  }, [fetchArticles]);

  // Load more articles (pagination)
  const loadPage = useCallback((page) => {
    // We need to keep track of current filters separately
    fetchArticles({}, page);
  }, [fetchArticles]);

  // Initial load - only run once
  useEffect(() => {
    fetchArticles({});
  }, [fetchArticles]);

  return { 
    articles, 
    loading, 
    error, 
    totalResults,
    currentPage,
    totalPages,
    searchArticles,
    loadPage,
    setArticles 
  };
};
