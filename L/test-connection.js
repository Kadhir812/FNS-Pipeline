import { config } from './src/config/config.js';
import client from './src/config/elasticsearch.js';

async function testConnection() {
  try {
    console.log('Testing Elasticsearch connection...');
    console.log('Elasticsearch URL:', config.elasticsearch.url);
    console.log('Index:', config.elasticsearch.index);
    
    // Test cluster health
    const health = await client.cluster.health();
    console.log('✅ Cluster health:', health.status);
    
    // Test index exists
    const indexExists = await client.indices.exists({ index: config.elasticsearch.index });
    console.log('✅ Index exists:', indexExists);
    
    if (indexExists) {
      // Get a sample document
      const response = await client.search({
        index: config.elasticsearch.index,
        body: {
          size: 1,
          query: {
            match_all: {}
          }
        }
      });
      
      console.log('✅ Sample document count:', response.hits.total.value);
      if (response.hits.hits.length > 0) {
        const sampleDoc = response.hits.hits[0];
        console.log('Sample document structure:');
        console.log('- ID:', sampleDoc._id);
        console.log('- Source keys:', Object.keys(sampleDoc._source));
        console.log('- Title:', sampleDoc._source.title?.substring(0, 50) + '...');
        console.log('- Published:', sampleDoc._source.publishedAt);
        console.log('- Category:', sampleDoc._source.category);
        console.log('- Source:', sampleDoc._source.source);
        console.log('- Sentiment:', sampleDoc._source.sentiment);
        console.log('- Risk Score:', sampleDoc._source.risk_score);
      }
    }
    
  } catch (error) {
    console.error('❌ Connection failed:', error.message);
  }
}

testConnection();
