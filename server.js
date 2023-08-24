const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const mysql = require('mysql2/promise');

const dbConfig = {
  host: 'localhost',
  user: 'lib',
  port: 4000,
  password: '(Password!123)',
  database: 'lib',
};

const csvFilePath = path.join(__dirname, 'public', '100K_cognisance_A2P.csv');

(async () => {
  try {
    console.log('Connecting to the database...');
    const connection = await mysql.createConnection(dbConfig);

    const batchSize = 1000;
    let dataBatch = [];
    let totalDeleted = 0;

    console.log('Reading and processing the CSV file...');
    fs.createReadStream(csvFilePath)
      .pipe(csvParser())
      .on('data', (row) => {
        const msisdn = '+' + row.msisdn;
        dataBatch.push(msisdn);

        if (dataBatch.length >= batchSize) {
          processBatch(dataBatch, connection);
          dataBatch = [];
        }
      })
      .on('end', async () => {
        if (dataBatch.length > 0) {
          await processBatch(dataBatch, connection);
        }

        console.log(`Total records deleted: ${totalDeleted}`);
        connection.end();
        console.timeEnd('Processing Time'); // End tracking time
        console.log('Process completed.');
      });
  } catch (error) {
    console.error('An error occurred:', error);
  }
})();

async function processBatch(batch, connection) {
  try {
    await connection.beginTransaction();
    const placeholders = batch.map(() => '?').join(',');
    const deleteQuery = `DELETE FROM getroutev3_clone WHERE destination_number IN (${placeholders})`;

    const [result] = await connection.query(deleteQuery, batch);
    await connection.commit();

    console.log(`Deleted ${result.affectedRows} records from this batch.`);
    totalDeleted += result.affectedRows;
  } catch (error) {
    await connection.rollback();
    console.error('An error occurred while processing batch:', error);
  }
}
