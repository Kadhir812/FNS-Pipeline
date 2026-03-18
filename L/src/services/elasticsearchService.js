import { Client } from '@elastic/elasticsearch';
import { config } from '../config/config.js';
import cache from '../utils/cache.js';

const client = new Client({
  node: process.env.ELASTICSEARCH_URL || 'http://elasticsearch:9200'
});

const INDEX_NAME = config.elasticsearch.index;

class ElasticsearchService {
  // Search articles with filters and sorting
  async searchArticles(query = {}) {
    try {
      // Add debug logging first
      console.log('Raw filter params:', query);

      const {
        q = '',
        sentiment = '',
        category = '',
        source = '',
        risk_level = '',
        start_date = '',
        end_date = '',
        sort_by = 'date',
        sort_order = 'desc',
        page = 1,
        page_size = 20
      } = query;

      // Log original and processed query params
      console.log('Original query params:', query);

      // Generate cache key from all query parameters
      const cacheKey = `search:${JSON.stringify({
        q, sentiment, category, source, risk_level, 
        start_date, end_date, sort_by, sort_order, page, page_size
      })}`;

      // Try cache first
      const cached = await cache.get(cacheKey);
      if (cached) {
        try {
          console.log('✅ Cache hit for search query');
          return JSON.parse(cached);
        } catch (e) {
          console.warn('Failed to parse cached search results, refetching', e.message);
        }
      }

      const must = [];
      const filter = [];

      // Enhanced search query for numbers and websites
      if (q && q.trim()) {
        must.push({
          multi_match: {
            query: q,
            fields: [
              'title^3',
              'description^2', 
              'content',
              'key_phrases^2',
              'summary^2',
              'source'  // Added source for website search
            ],
            type: 'best_fields',
            fuzziness: 'AUTO'
          }
        });
      } else {
        must.push({
          match_all: {}
        });
      }

      // Sentiment filter
      if (sentiment) {
        console.log('Filtering by sentiment:', sentiment);
        if (sentiment === 'positive') {
          filter.push({
            range: { sentiment: { gt: 0 } }  // All positive values
          });
        } else if (sentiment === 'negative') {
          filter.push({
            range: { sentiment: { lt: 0 } }  // All negative values
          });
        } else if (sentiment === 'neutral') {
          filter.push({
            range: { sentiment: { gte: -0.05, lte: 0.05 } }  // Near zero
          });
        }
      }

      // Category filter
      if (category) {
        // First try to debug what's happening
        console.log('Filtering by category:', category);
        
        // If category is a code (A-J) or text label
        if (category.length === 1 && /^[A-J]$/i.test(category)) {
          // For single letter categories
          filter.push({
            term: { category: category.toUpperCase() }
          });
        } else {
          // For text categories (try both term and match)
          filter.push({
            bool: {
              should: [
                { term: { 'category.keyword': category } },
                { match: { category: category } }
              ]
            }
          });
        }
      }

      // Source filter
      if (source) {
        console.log('Filtering by source:', source);
        // Try multiple approaches to match the source
        filter.push({
          bool: {
            should: [
              { term: { 'source.keyword': source } }, // Try keyword field first
              { term: { 'source': source } },         // Try exact match on text field
              { match: { 'source': source } }         // Try fuzzy matching
            ],
            minimum_should_match: 1
          }
        });
      }

      // Risk level filter - Use the risk_level field from Transform.py
      if (risk_level) {
        console.log('Filtering by risk level:', risk_level);
        // Filter by the actual risk_level field from Transform.py
        filter.push({
          term: { risk_level: risk_level }
        });
      }

      // Date range filter
      if (start_date || end_date) {
        const dateFilter = {
          range: {
            publishedAt: {}
          }
        };
        
        if (start_date) {
          dateFilter.range.publishedAt.gte = new Date(start_date).getTime();
        }
        if (end_date) {
          dateFilter.range.publishedAt.lte = new Date(end_date).getTime();
        }
        
        filter.push(dateFilter);
      }

      // Build dynamic sort array based on sort_by parameter
      const sortArray = [];
      
      // Map sort_by to Elasticsearch field names
      const sortFieldMap = {
        'date': 'publishedAt',
        'publishedAt': 'publishedAt',
        'published_date': 'publishedAt',
        'relevance': '_score',
        'sentiment': 'sentiment',
        'risk': 'risk_score',
        'risk_score': 'risk_score',
        'confidence': 'confidence'
      };
      
      const elasticField = sortFieldMap[sort_by] || 'publishedAt';
      
      if (elasticField === '_score') {
        // For relevance sorting, score comes first
        sortArray.push({ "_score": { order: sort_order === 'asc' ? 'asc' : 'desc' } });
        sortArray.push({ "publishedAt": { order: "desc" } }); // Secondary sort by date
      } else {
        // For other fields, sort by the field first, then by relevance
        sortArray.push({ [elasticField]: { order: sort_order === 'asc' ? 'asc' : 'desc' } });
        if (elasticField !== 'publishedAt') {
          sortArray.push({ "publishedAt": { order: "desc" } }); // Secondary sort by date for non-date fields
        }
        sortArray.push({ "_score": { order: "desc" } }); // Tertiary sort by relevance
      }

      const searchBody = {
        query: {
          bool: {
            must: must.length > 0 ? must : [{ match_all: {} }],
            filter: filter,
            should: [
              // Boost articles with symbols
              {
                exists: {
                  field: "symbol",
                  boost: 2.0
                }
              },
              // Boost articles with entity names
              {
                exists: {
                  field: "entity_name",
                  boost: 1.5
                }
              }
            ]
          }
        },
        sort: sortArray,
        from: (page - 1) * page_size,
        size: Math.min(page_size, 100), // Limit max page size
        highlight: {
          fields: {
            title: {},
            description: {},
            content: { fragment_size: 150 },
            key_phrases: {},
            source: {}  // Added source highlighting
          }
        }
      };

      console.log('Elasticsearch query:', JSON.stringify(searchBody, null, 2));
      console.log('🔄 Sort configuration:', { sort_by, sort_order, elasticField, sortArray });

      const response = await client.search({
        index: INDEX_NAME,
        body: searchBody
      });

      // Log processed query params after processing
      console.log('Processed query params:', query);

      // Log the number of articles found
      console.log(`Search returned ${response.hits.hits.length} articles out of ${response.hits.total.value} total`);

      const result = {
        articles: response.hits.hits.map(hit => ({
          ...hit._source,
          _id: hit._id,
          _score: hit._score,
          highlight: hit.highlight
        })),
        total: response.hits.total.value,
        page: parseInt(page),
        page_size: parseInt(page_size),
        total_pages: Math.ceil(response.hits.total.value / page_size)
      };

      // Cache the result (60 seconds TTL for search results)
      try {
        await cache.set(cacheKey, JSON.stringify(result), 60);
        console.log('✅ Cached search results');
      } catch (e) {
        console.warn('Failed to cache search results:', e.message);
      }

      return result;
    } catch (error) {
      console.error('Error searching articles:', error);
      throw new Error('Failed to search articles');
    }
  }

  // Get article by ID
  async getArticleById(id) {
    try {
      // Cache key for individual articles
      const cacheKey = `article:${id}`;

      // Try cache first
      const cached = await cache.get(cacheKey);
      if (cached) {
        try {
          console.log(`✅ Cache hit for article ${id}`);
          return JSON.parse(cached);
        } catch (e) {
          console.warn('Failed to parse cached article, refetching', e.message);
        }
      }

      const response = await client.get({
        index: INDEX_NAME,
        id
      });

      const result = {
        id: response._id,
        ...response._source
      };

      // Cache the article (5 minutes TTL)
      try {
        await cache.set(cacheKey, JSON.stringify(result), 300);
        console.log(`✅ Cached article ${id}`);
      } catch (e) {
        console.warn('Failed to cache article:', e.message);
      }

      return result;
    } catch (error) {
      if (error.statusCode === 404) {
        return null;
      }
      console.error('Error getting article by ID:', error);
      throw new Error('Failed to get article');
    }
  }

  // Get aggregations for filters
  async getAggregations() {
    try {
      // Cache key for aggregations
      const cacheKey = 'aggregations:filters';

      // Try cache first
      const cached = await cache.get(cacheKey);
      if (cached) {
        try {
          console.log('✅ Cache hit for aggregations');
          return JSON.parse(cached);
        } catch (e) {
          console.warn('Failed to parse cached aggregations, refetching', e.message);
        }
      }

      const response = await client.search({
        index: INDEX_NAME,
        body: {
          size: 0,
          aggs: {
            categories: {
              terms: { 
                field: 'category',
                size: 50
            }
            },
            sources: {
              terms: { 
                field: 'source',
                size: 50
            }
            }
          }
        }
      });

      const result = {
        categories: response.aggregations.categories.buckets.map(bucket => bucket.key),
        sources: response.aggregations.sources.buckets.map(bucket => bucket.key)
      };

      // Cache aggregations (10 minutes TTL - filters don't change frequently)
      try {
        await cache.set(cacheKey, JSON.stringify(result), 600);
        console.log('✅ Cached aggregations');
      } catch (e) {
        console.warn('Failed to cache aggregations:', e.message);
      }

      return result;
    } catch (error) {
      console.error('Aggregations error:', error);
      return { categories: [], sources: [] };
    }
  }

  // Get similar articles based on key phrases and category
  async getSimilarArticles(articleId, limit = 5) {
    try {
      // Cache key for similar articles
      const cacheKey = `similar:${articleId}:${limit}`;

      // Try cache first
      const cached = await cache.get(cacheKey);
      if (cached) {
        try {
          console.log(`✅ Cache hit for similar articles to ${articleId}`);
          return JSON.parse(cached);
        } catch (e) {
          console.warn('Failed to parse cached similar articles, refetching', e.message);
        }
      }

      // First, get the article to extract key phrases
      const article = await this.getArticleById(articleId);
      if (!article) {
        return [];
      }

      const keyPhrases = article.key_phrases ? article.key_phrases.split(',').map(p => p.trim()) : [];
      
      const response = await client.search({
        index: INDEX_NAME,
        body: {
          size: limit + 1, // +1 to exclude the original article
          query: {
            bool: {
              should: [
                {
                  terms: {
                    'category.keyword': [article.category]
                  }
                },
                {
                  multi_match: {
                    query: keyPhrases.join(' '),
                    fields: ['key_phrases^2', 'title', 'description']
                  }
                }
              ],
              must_not: {
                term: { _id: articleId }
              },
              minimum_should_match: 1
            }
          },
          sort: [
            { _score: { order: 'desc' } },
            { publishedAt: { order: 'desc' } }
          ]
        }
      });

      const result = response.hits.hits.slice(0, limit).map(hit => ({
        _id: hit._id,
        ...hit._source,
        _score: hit._score
      }));

      // Cache similar articles (5 minutes TTL)
      try {
        await cache.set(cacheKey, JSON.stringify(result), 300);
        console.log(`✅ Cached similar articles for ${articleId}`);
      } catch (e) {
        console.warn('Failed to cache similar articles:', e.message);
      }

      return result;
    } catch (error) {
      console.error('Error getting similar articles:', error);
      throw new Error('Failed to get similar articles');
    }
  }

  // Health check
  async healthCheck() {
    try {
      const health = await client.cluster.health();
      const indexExists = await client.indices.exists({ index: INDEX_NAME });
      
      return {
        elasticsearch: health.status,
        index: indexExists ? 'exists' : 'missing',
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error('Health check failed:', error);
      return {
        elasticsearch: 'error',
        index: 'unknown',
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  // Add a method to inspect the index mapping
  async getIndexMapping() {
    try {
      return await client.indices.getMapping({
        index: INDEX_NAME
      });
    } catch (error) {
      console.error('Error getting index mapping:', error);
      throw new Error('Failed to get index mapping');
    }
  }

  // Get time-series history for a given symbol (final_score and timestamp)
  // Params: symbol (string), limit (int), from (epoch ms) optional, to (epoch ms) optional
  async getArticleHistory(symbol, limit = 100, from = null, to = null) {
    if (!symbol) return [];
    try {
      const keyParts = ['history', symbol.trim().toUpperCase(), from || 'na', to || 'na', limit];
      const cacheKey = keyParts.join(':');

      // Try cache first
      const cached = await cache.get(cacheKey);
      if (cached) {
        try {
          return JSON.parse(cached);
        } catch (e) {
          // fall through to re-query
          console.warn('Failed to parse cached history, refetching', e.message);
        }
      }

      // Build ES query
      const must = [
        { term: { 'symbol.keyword': symbol.trim().toUpperCase() } }
      ];

      const filter = [];
      if (from || to) {
        const range = { range: { publishedAt: {} } };
        if (from) range.range.publishedAt.gte = Number(from);
        if (to) range.range.publishedAt.lte = Number(to);
        filter.push(range);
      }

      const response = await client.search({
        index: INDEX_NAME,
        body: {
          query: {
            bool: {
              must,
              filter
            }
          },
          sort: [{ publishedAt: { order: 'asc' } }],
          size: Math.min(limit, 10000),
          _source: ['publishedAt', 'final_score']
        }
      });

      const rows = response.hits.hits.map(h => ({
        ts: h._source.publishedAt,
        final_score: typeof h._source.final_score === 'number' ? h._source.final_score : (h._source.final_score ? Number(h._source.final_score) : null)
      })).filter(r => r.final_score !== null && r.ts !== undefined);

      // Cache the result (stringified)
      try {
        await cache.set(cacheKey, JSON.stringify(rows), Number(process.env.HISTORY_CACHE_TTL || 120));
      } catch (e) {
        // ignore cache errors
      }

      return rows;
    } catch (error) {
      console.error('Error getting article history:', error);
      return [];
    }
  }
}

export default new ElasticsearchService();
