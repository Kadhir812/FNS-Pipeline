import { articleAPI } from './src/services/api.js';

async function testAPI() {
  try {
    console.log('Testing API search endpoint...');
    
    const response = await articleAPI.searchArticles({
      page: 1,
      page_size: 5
    });
    
    console.log('API Response:', JSON.stringify(response, null, 2));
    
    if (response.success) {
      console.log('✅ API working! Found', response.data.total, 'articles');
      console.log('✅ First article:', response.data.articles[0]?.title);
    } else {
      console.log('❌ API failed:', response);
    }
    
  } catch (error) {
    console.error('❌ API Error:', error.message);
  }
}

testAPI();
