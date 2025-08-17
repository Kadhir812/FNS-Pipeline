# SignalEdge API Backend

A robust Express.js backend API for the SignalEdge financial intelligence platform, providing real-time news analysis and filtering capabilities through Elasticsearch integration.

## Features

- **RESTful API** - Clean, well-documented endpoints
- **Elasticsearch Integration** - Full-text search and aggregations
- **Rate Limiting** - Protection against abuse
- **Security Headers** - Helmet.js for security
- **CORS Support** - Cross-origin resource sharing
- **Logging** - Winston-based structured logging
- **Input Validation** - Joi schema validation
- **Error Handling** - Comprehensive error middleware

## API Endpoints

### Health Check
- `GET /api/v1/health/health` - Full health check with Elasticsearch status
- `GET /api/v1/health/ping` - Simple ping endpoint

### Articles
- `GET /api/v1/articles/search` - Search articles with filters
- `GET /api/v1/articles/:id` - Get specific article by ID
- `GET /api/v1/articles/:id/similar` - Get similar articles
- `GET /api/v1/articles/meta/aggregations` - Get filter metadata

## Quick Start

### Prerequisites
- Node.js 18.0.0 or higher
- Elasticsearch 8.x running on localhost:9200
- npm or yarn package manager

### Installation

1. **Clone and navigate to the backend directory:**
   ```bash
   cd L
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Set up environment variables:**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` file with your configuration.

4. **Create logs directory:**
   ```bash
   mkdir logs
   ```

5. **Start the development server:**
   ```bash
   npm run dev
   ```

The API server will start on `http://localhost:3001`

### Production Deployment

```bash
npm start
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ENV` | development | Environment mode |
| `PORT` | 3001 | Server port |
| `HOST` | localhost | Server host |
| `ELASTICSEARCH_HOST` | http://localhost:9200 | Elasticsearch URL |
| `ELASTICSEARCH_INDEX` | news-analysis | Elasticsearch index name |
| `LOG_LEVEL` | info | Logging level |

### Elasticsearch Setup

Ensure your Elasticsearch instance has a `news-analysis` index with the following mapping structure:

```json
{
  "mappings": {
    "properties": {
      "title": { "type": "text" },
      "content": { "type": "text" },
      "url": { "type": "keyword" },
      "published_date": { "type": "date" },
      "source": { "type": "keyword" },
      "sentiment_score": { "type": "float" },
      "risk_score": { "type": "float" },
      "category": { "type": "keyword" },
      "tags": { "type": "keyword" }
    }
  }
}
```

## API Documentation

### Search Articles

**Endpoint:** `GET /api/v1/articles/search`

**Query Parameters:**
- `q` - Search query (optional)
- `source` - Filter by news source (optional)
- `category` - Filter by category (optional)
- `sentiment` - Filter by sentiment (positive/negative/neutral) (optional)
- `risk_level` - Filter by risk level (low/medium/high) (optional)
- `start_date` - Filter from date (YYYY-MM-DD) (optional)
- `end_date` - Filter to date (YYYY-MM-DD) (optional)
- `page` - Page number (default: 1)
- `page_size` - Results per page (default: 20, max: 100)
- `sort_by` - Sort field (date/relevance/sentiment/risk) (default: date)
- `sort_order` - Sort direction (asc/desc) (default: desc)

**Example Response:**
```json
{
  "success": true,
  "data": {
    "articles": [...],
    "total": 1250,
    "page": 1,
    "page_size": 20,
    "total_pages": 63
  },
  "timestamp": "2024-01-15T10:30:00.000Z"
}
```

### Get Article by ID

**Endpoint:** `GET /api/v1/articles/:id`

**Example Response:**
```json
{
  "success": true,
  "data": {
    "id": "article_123",
    "title": "Market Analysis Update",
    "content": "...",
    "source": "Reuters",
    "published_date": "2024-01-15T09:00:00.000Z",
    "sentiment_score": 0.75,
    "risk_score": 0.3
  }
}
```

## Development

### Scripts

- `npm start` - Start production server
- `npm run dev` - Start development server with hot reload
- `npm test` - Run test suite
- `npm run lint` - Run ESLint
- `npm run lint:fix` - Fix ESLint issues

### Project Structure

```
L/
├── src/
│   ├── config/           # Configuration files
│   ├── controllers/      # Route controllers
│   ├── middleware/       # Express middleware
│   ├── routes/          # API routes
│   ├── services/        # Business logic services
│   └── utils/           # Utility functions
├── logs/                # Log files
├── server.js           # Application entry point
└── package.json        # Dependencies and scripts
```

### Error Handling

The API implements comprehensive error handling:

- **400 Bad Request** - Invalid input parameters
- **404 Not Found** - Resource not found
- **429 Too Many Requests** - Rate limit exceeded
- **500 Internal Server Error** - Server errors
- **503 Service Unavailable** - Elasticsearch connection issues

## Security Features

- **Helmet.js** - Security headers
- **CORS** - Cross-origin resource sharing
- **Rate Limiting** - Request throttling
- **Input Validation** - Joi schema validation
- **Error Sanitization** - Prevents information leakage

## Monitoring and Logging

- **Winston** - Structured logging
- **Health Checks** - Elasticsearch connectivity monitoring
- **Request Logging** - All API requests logged
- **Error Tracking** - Comprehensive error logging

## License

MIT License - see LICENSE file for details.

## Support

For support and questions, please contact the SignalEdge development team.
