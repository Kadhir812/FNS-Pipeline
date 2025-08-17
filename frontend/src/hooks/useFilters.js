import { useState, useEffect } from 'react';
import { articleAPI } from '../services/api';

export const useFilters = () => {
  const [filters, setFilters] = useState({
    dateRange: { start: '', end: '' },
    sentiment: 'all',
    riskLevel: 'all',
    category: 'all',
    source: 'all',
    search: '',
    sortBy: 'date',
    sortOrder: 'desc'
  });

  const [availableFilters, setAvailableFilters] = useState({
    sources: [],
    categories: [],
    sentiments: ['positive', 'negative', 'neutral'],
    riskLevels: ['low', 'medium', 'high']
  });

  // Fetch available filter options from API
  useEffect(() => {
    const fetchFilterMetadata = async () => {
      try {
        const response = await articleAPI.getFilterMetadata();
        if (response.success) {
          setAvailableFilters({
            sources: response.data.sources || [],
            categories: response.data.categories || [],
            sentiments: ['positive', 'negative', 'neutral'],
            riskLevels: ['low', 'medium', 'high']
          });
        }
      } catch (error) {
        console.error('Error fetching filter metadata:', error);
        // Keep default values if API fails
      }
    };

    fetchFilterMetadata();
  }, []);

  const updateFilter = (key, value) => {
    setFilters(prev => ({
      ...prev,
      [key]: value
    }));
  };

  const resetFilters = () => {
    setFilters({
      dateRange: { start: '', end: '' },
      sentiment: 'all',
      riskLevel: 'all',
      category: 'all',
      source: 'all',
      search: '',
      sortBy: 'date',
      sortOrder: 'desc'
    });
  };

  // Convert filters to API format
  const getAPIFilters = () => {
    const apiFilters = {};
    
    if (filters.search) apiFilters.q = filters.search;
    if (filters.source !== 'all') apiFilters.source = filters.source;
    if (filters.category !== 'all') apiFilters.category = filters.category;
    if (filters.sentiment !== 'all') apiFilters.sentiment = filters.sentiment;
    if (filters.riskLevel !== 'all') apiFilters.risk_level = filters.riskLevel;
    if (filters.dateRange.start) apiFilters.start_date = filters.dateRange.start;
    if (filters.dateRange.end) apiFilters.end_date = filters.dateRange.end;
    if (filters.sortBy) apiFilters.sort_by = filters.sortBy;
    if (filters.sortOrder) apiFilters.sort_order = filters.sortOrder;
    
    return apiFilters;
  };

  return { 
    filters, 
    setFilters, 
    updateFilter, 
    resetFilters, 
    availableFilters,
    getAPIFilters 
  };
};
