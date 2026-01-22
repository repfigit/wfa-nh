import { dbHelpers } from '../../src/db/database.js';

async function testDataSources() {
  try {
    console.log('Testing data sources loading...');
    const dataSources = await dbHelpers.getDataSources();
    console.log(`Found ${dataSources.length} data sources:`);
    dataSources.forEach(ds => {
      console.log(`- ${ds.name}: ${ds.url}`);
    });
  } catch (error) {
    console.error('Error loading data sources:', error);
  }
}

testDataSources().then(() => {
  console.log('Test completed');
  process.exit(0);
}).catch(error => {
  console.error('Test failed:', error);
  process.exit(1);
});