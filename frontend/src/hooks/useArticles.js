import { useState, useEffect, useCallback } from 'react';
import { articleAPI } from '../services/api';

export const useArticles = (initialFilters = {}) => {
  const [articles, setArticles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [totalResults, setTotalResults] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);
  const [currentFilters, setCurrentFilters] = useState(initialFilters);

  const fetchArticles = useCallback(async (searchFilters = {}, page = 1) => {
    try {
      setLoading(true);
      setError(null);
      
      const searchParams = {
        ...searchFilters,
        page,
        page_size: 20, // Default page size
      };

      console.log('🔍 Fetching articles with params:', searchParams);
      // Debug: Log sentiment filter value
      if (searchParams.sentiment) {
        console.log('Sentiment filter value sent to API:', searchParams.sentiment);
      }

      const response = await articleAPI.searchArticles(searchParams);
      
      if (response.success) {
        setArticles(response.data.articles || []);
        setTotalResults(response.data.total || 0);
        setCurrentPage(response.data.page || 1);
        setTotalPages(response.data.total_pages || 0);
        setCurrentFilters(searchFilters); // Store current filters for pagination
        console.log('✅ Articles loaded:', response.data.articles?.length || 0);
      } else {
        setError('Failed to fetch articles');
        console.error('❌ API response failed:', response);
      }
    } catch (err) {
      console.error('❌ Error fetching articles:', err);
      setError(err.message || 'Failed to fetch articles');
      setArticles([]);
    } finally {
      setLoading(false);
    }
  }, []);

  // Search with new filters
  const searchArticles = useCallback((newFilters) => {
    console.log('🔍 Searching with new filters:', newFilters);
    fetchArticles(newFilters, 1);
  }, [fetchArticles]);

  // Load more articles (pagination) - maintain current filters
  const loadPage = useCallback((page) => {
    console.log('📄 Loading page:', page, 'with filters:', currentFilters);
    fetchArticles(currentFilters, page);
  }, [fetchArticles, currentFilters]);

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
