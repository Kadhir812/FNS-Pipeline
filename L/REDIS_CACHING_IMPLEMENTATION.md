# Redis Caching Implementation for Elasticsearch APIs

## Overview
Added Redis caching layer to all Elasticsearch API endpoints to improve performance and reduce database load.

## Cached Endpoints

### 1. Search Articles (`/api/v1/articles/search`)
- **Cache Key Pattern**: `search:{query_params_json}`
- **TTL**: 60 seconds
- **Benefit**: Reduces repeated search queries with identical filters
- **Cache Key Example**: `search:{"q":"apple","sentiment":"positive","page":1,"page_size":20}`

### 2. Get Article by ID (`/api/v1/articles/:id`)
- **Cache Key Pattern**: `article:{article_id}`
- **TTL**: 5 minutes (300 seconds)
- **Benefit**: Individual articles rarely change, can be cached longer
- **Cache Key Example**: `article:abc123def456`

### 3. Get Aggregations (`/api/v1/articles/meta/aggregations`)
- **Cache Key Pattern**: `aggregations:filters`
- **TTL**: 10 minutes (600 seconds)
- **Benefit**: Filter options change infrequently, long cache time
- **Cache Key Example**: `aggregations:filters`

### 4. Get Similar Articles (`/api/v1/articles/:id/similar`)
- **Cache Key Pattern**: `similar:{article_id}:{limit}`
- **TTL**: 5 minutes (300 seconds)
- **Benefit**: Similar article recommendations are stable
- **Cache Key Example**: `similar:abc123:5`

### 5. Get Article History (`/api/v1/articles/history`)
- **Cache Key Pattern**: `history:{symbol}:{from}:{to}:{limit}`
- **TTL**: 2 minutes (120 seconds) - configurable via `HISTORY_CACHE_TTL` env var
- **Benefit**: Time-series data for charts
- **Cache Key Example**: `history:AAPL:na:na:200`

## Cache Utility Functions

### Available Methods
```javascript
import cache from './utils/cache.js';

// Get cached value
const value = await cache.get('key');

// Set cached value with TTL
await cache.set('key', 'value', 60); // 60 seconds

// Delete single key
await cache.del('key');

// Clear all keys matching pattern
await cache.clearPattern('search:*'); // Clear all search caches

// Check if Redis is available
const isAvailable = cache.isAvailable();
```

## Performance Impact

### Expected Benefits
- **Search queries**: 60-90% reduction in ES load for repeated searches
- **Article fetching**: Instant response for cached articles
- **Aggregations**: Near-instant filter options loading
- **Similar articles**: Faster recommendation loading

### Cache Hit Indicators
Look for console logs:
- `✅ Cache hit for search query`
- `✅ Cache hit for article {id}`
- `✅ Cache hit for aggregations`
- `✅ Cache hit for similar articles to {id}`

### Cache Miss Behavior
If Redis is unavailable or cache miss occurs:
- Falls back to direct Elasticsearch query
- No errors thrown - graceful degradation
- Logs warnings for debugging

## Configuration

### Environment Variables
```env
# Redis connection URL
REDIS_URL=redis://localhost:6379

# History cache TTL (seconds)
HISTORY_CACHE_TTL=120
```

### Redis Installation
Ensure Redis is running:
```bash
# Windows (using WSL or Redis for Windows)
redis-server

# Check connection
redis-cli ping
# Should return: PONG
```

### NPM Dependencies
```bash
npm install redis
```

## Monitoring Cache Performance

### Redis CLI Commands
```bash
# Check all cache keys
redis-cli KEYS "*"

# Monitor cache activity in real-time
redis-cli MONITOR

# Get cache statistics
redis-cli INFO stats

# Check specific key TTL
redis-cli TTL "search:{...}"

# Clear all cache
redis-cli FLUSHDB
```

### Programmatic Cache Clearing
```javascript
// Clear all search caches
await cache.clearPattern('search:*');

// Clear all article caches
await cache.clearPattern('article:*');

// Clear all caches
await cache.clearPattern('*');
```

## Cache Strategy

### TTL Guidelines
- **Short TTL (60s)**: Search results (data changes frequently)
- **Medium TTL (5 min)**: Individual articles, similar articles
- **Long TTL (10 min)**: Aggregations (filter options)
- **Custom TTL**: History (configurable via env var)

### When to Invalidate Cache
- After new articles indexed to Elasticsearch
- After article updates
- On demand via admin endpoint (future enhancement)

## Future Enhancements

1. **Cache Warming**: Pre-populate cache with common queries
2. **Cache Invalidation API**: Endpoint to clear specific cache patterns
3. **Cache Analytics**: Track hit/miss ratio, popular queries
4. **Distributed Caching**: Redis Cluster for high availability
5. **Cache Versioning**: Invalidate cache when schema changes

## Troubleshooting

### Redis Connection Issues
If you see: `Redis unavailable - continuing without cache`
- Check if Redis server is running
- Verify `REDIS_URL` in `.env`
- Check firewall/network settings

### Cache Not Working
- Verify Redis connection: `redis-cli ping`
- Check logs for cache errors
- Ensure `redis` npm package is installed
- Verify TTL values are appropriate

### Performance Not Improving
- Monitor cache hit rate via logs
- Increase TTL for stable data
- Check Redis memory usage
- Consider cache warming for popular queries
