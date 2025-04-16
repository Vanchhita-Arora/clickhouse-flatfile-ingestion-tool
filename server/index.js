import express from 'express';
import cors from 'cors';
import multer from 'multer';
import { createClient } from '@clickhouse/client';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import fs from 'fs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const upload = multer({ dest: 'uploads/' });
const app = express();

app.use(cors());
app.use(express.json());

// ClickHouse connection helper
const createClickHouseClient = (config) => {
  return createClient({
    host: `http://${config.host}:${config.port}`,
    database: config.database,
    username: config.username,
    password: config.token,
  });
};

// Test connection and get tables
app.post('/api/connect', async (req, res) => {
  try {
    const { config } = req.body;
    const client = createClickHouseClient(config);
    
    // Test connection
    const tables = await client.query({
      query: `
        SELECT name, type
        FROM system.columns
        WHERE database = '${config.database}'
        ORDER BY table, position
      `,
    });
    
    const result = await tables.json();
    
    // Transform the result into the expected format
    const tableMap = new Map();
    result.data.forEach(row => {
      if (!tableMap.has(row.table)) {
        tableMap.set(row.table, {
          name: row.table,
          columns: [],
        });
      }
      tableMap.get(row.table).columns.push({
        name: row.name,
        type: row.type,
        selected: false,
      });
    });
    
    await client.close();
    
    res.json({ tables: Array.from(tableMap.values()) });
  } catch (error) {
    console.error('Connection error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Ingest from ClickHouse to Flat File
app.post('/api/ingest/clickhouse', async (req, res) => {
  try {
    const { config, selectedTable, columns } = req.body;
    const client = createClickHouseClient(config);
    
    const columnNames = columns.map(c => c.name).join(', ');
    const query = `SELECT ${columnNames} FROM ${selectedTable}`;
    
    const result = await client.query({
      query,
      format: 'JSONEachRow',
    });
    
    const data = await result.json();
    
    // Write to file
    const outputPath = join(__dirname, 'output', `${selectedTable}_export.csv`);
    const writer = fs.createWriteStream(outputPath);
    
    // Write header
    writer.write(columns.map(c => c.name).join(',') + '\n');
    
    // Write data
    let recordCount = 0;
    for (const row of data) {
      writer.write(
        columns.map(c => row[c.name]).join(',') + '\n'
      );
      recordCount++;
    }
    
    writer.end();
    await client.close();
    
    res.json({ recordCount });
  } catch (error) {
    console.error('Ingestion error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Ingest from Flat File to ClickHouse
app.post('/api/ingest/flatfile', upload.single('file'), async (req, res) => {
  try {
    const { config } = req.body;
    const file = req.file;
    
    if (!file) {
      throw new Error('No file uploaded');
    }
    
    // Read file content
    const content = fs.readFileSync(file.path, 'utf-8');
    const lines = content.split('\n');
    const headers = lines[0].split(config.delimiter);
    
    // Create table in ClickHouse
    const client = createClickHouseClient(JSON.parse(req.body.config));
    const tableName = `import_${Date.now()}`;
    
    // Assume string type for all columns for simplicity
    const createTableQuery = `
      CREATE TABLE ${tableName} (
        ${headers.map(h => `${h.trim()} String`).join(', ')}
      ) ENGINE = MergeTree() ORDER BY tuple()
    `;
    
    await client.query({ query: createTableQuery });
    
    // Insert data
    let recordCount = 0;
    for (let i = 1; i < lines.length; i++) {
      if (!lines[i].trim()) continue;
      
      const values = lines[i].split(config.delimiter);
      const insertQuery = `
        INSERT INTO ${tableName}
        VALUES (${values.map(v => `'${v.replace(/'/g, "''")}'`).join(', ')})
      `;
      
      await client.query({ query: insertQuery });
      recordCount++;
    }
    
    // Cleanup
    fs.unlinkSync(file.path);
    await client.close();
    
    res.json({ recordCount });
  } catch (error) {
    console.error('Ingestion error:', error);
    res.status(500).json({ error: error.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});