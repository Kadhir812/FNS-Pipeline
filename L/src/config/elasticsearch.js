import { Client } from '@elastic/elasticsearch';
import dotenv from 'dotenv';

dotenv.config();

const client = new Client({
  node: process.env.ELASTICSEARCH_URL || 'http://localhost:9200',
//   auth: process.env.ELASTICSEARCH_USERNAME && process.env.ELASTICSEARCH_PASSWORD ? {
//     // username: process.env.ELASTICSEARCH_USERNAME,
//     // password: process.env.ELASTICSEARCH_PASSWORD
//   } : undefined,
  requestTimeout: 30000,
  maxRetries: 3
});

// Test connection
export const testConnection = async () => {
  try {
    const health = await client.cluster.health();
    console.log('✅ Elasticsearch connection successful:', health.status);
    return true;
  } catch (error) {
    console.error('❌ Elasticsearch connection failed:', error.message);
    return false;
  }
};

// Check if index exists
export const checkIndex = async (index) => {
  try {
    const exists = await client.indices.exists({ index });
    return exists;
  } catch (error) {
    console.error(`Error checking index ${index}:`, error.message);
    return false;
  }
};

export default client;
