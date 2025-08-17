import { Client } from '@elastic/elasticsearch';
import { config } from '../config/config.js';

const client = new Client({
  node: process.env.ELASTICSEARCH_URL || 'http://localhost:9200'
});

const INDEX_NAME = config.elasticsearch.index;

class ElasticsearchService {
  // Search articles with filters and sorting
  async searchArticles(query = {}) {
    try {
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
        if (sentiment === 'positive') {
          filter.push({
            range: { sentiment: { gt: 0.1 } }
          });
        } else if (sentiment === 'negative') {
          filter.push({
            range: { sentiment: { lt: -0.1 } }
          });
        } else if (sentiment === 'neutral') {
          filter.push({
            range: { 
              sentiment: { 
                gte: -0.1,
                lte: 0.1 
              } 
            }
          });
        }
      }

      // Category filter
      if (category) {
        filter.push({
          term: { 'category.keyword': category }
        });
      }

      // Source filter
      if (source) {
        filter.push({
          term: { 'source.keyword': source }
        });
      }

      // Risk level filter
      if (risk_level) {
        let riskRange = {};
        if (risk_level === 'low') {
          riskRange = { lt: 0.33 };
        } else if (risk_level === 'medium') {
          riskRange = { gte: 0.33, lt: 0.67 };
        } else if (risk_level === 'high') {
          riskRange = { gte: 0.67 };
        }
        
        if (Object.keys(riskRange).length > 0) {
          filter.push({
            range: { risk_score: riskRange }
          });
        }
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

      const searchBody = {
        query: {
          bool: {
            must: must.length > 0 ? must : [{ match_all: {} }],
            filter: filter
          }
        },
        sort: [
          { publishedAt: { order: sort_order === 'asc' ? 'asc' : 'desc' } }
        ],
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

      const response = await client.search({
        index: INDEX_NAME,
        body: searchBody
      });

      return {
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
    } catch (error) {
      console.error('Error searching articles:', error);
      throw new Error('Failed to search articles');
    }
  }

  // Get article by ID
  async getArticleById(id) {
    try {
      const response = await client.get({
        index: INDEX_NAME,
        id
      });

      return {
        id: response._id,
        ...response._source
      };
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

      return {
        categories: response.aggregations.categories.buckets.map(bucket => bucket.key),
        sources: response.aggregations.sources.buckets.map(bucket => bucket.key)
      };
    } catch (error) {
      console.error('Aggregations error:', error);
      return { categories: [], sources: [] };
    }
  }

  // Get similar articles based on key phrases and category
  async getSimilarArticles(articleId, limit = 5) {
    try {
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

      return response.hits.hits.slice(0, limit).map(hit => ({
        _id: hit._id,
        ...hit._source,
        _score: hit._score
      }));
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
}

export default new ElasticsearchService();
