const express = require('express');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const path = require('path');
const app = express();
const port = 5506;

app.use(bodyParser.urlencoded({ extended: true }));

const pool = mysql.createPool({
  host: process.env.DB_HOST || 'db',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || 'root',
  database: process.env.DB_NAME || 'dadd_db',
  port: process.env.DB_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  connectTimeout: 60000,
  queueLimit: 0
});

// 測試資料庫連線（帶重試機制）
let isDbConnected = false;
function testConnection(retries = 20) {
  pool.getConnection((err, connection) => {
    if (err) {
      if (retries === 20) {
        console.log('⏳ Waiting for database initialization...');
      }
      if (retries > 0) {
        setTimeout(() => testConnection(retries - 1), 5000);
      } else {
        console.error('❌ Failed to connect to database after 100 seconds');
        console.error('💡 Connection error:', err.message);
        process.exit(1);
      }
    } else {
      if (!isDbConnected) {
        console.log('✅ Database connected successfully!');
        console.log('\n🚀 DADD Analytics Dashboard is ready!');
        console.log(`📊 Access the application at: \x1b[36mhttp://localhost:${port}\x1b[0m`);
        console.log(`🌐 Or click here: \x1b[4m\x1b[36mhttp://localhost:${port}\x1b[0m\n`);
        isDbConnected = true;
      }
      connection.release();
    }
  });
}

// testConnection will be called after server starts

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// ==========================================
// 選單 API (加入 DISTINCT 防止重複)
// ==========================================
app.get('/api/countries', (req, res) => {
  pool.query('SELECT DISTINCT code, name FROM countries ORDER BY name ASC', (err, results) => {
    if (err) {
      console.error('❌ /api/countries error:', err.message);
      return res.send('<option>Error loading data</option>');
    }
    if (!results || results.length === 0) {
      console.warn('⚠️ No countries found in database');
      return res.send('<option>No countries found</option>');
    }
    let options = '<option value="" selected>Select a Country</option>';
    results.forEach(r => options += `<option value="${r.code}">${r.name}</option>`);
    res.send(options);
  });
});

app.get('/api/regions', (req, res) => {
  pool.query('SELECT DISTINCT id, name FROM regions ORDER BY name ASC', (err, results) => {
    if (err) {
      console.error('❌ /api/regions error:', err.message);
      return res.send('<option>Error loading data</option>');
    }
    if (!results || results.length === 0) {
      console.warn('⚠️ No regions found in database');
      return res.send('<option>No regions found</option>');
    }
    let options = '<option value="" selected>Select Region</option>';
    results.forEach(r => options += `<option value="${r.id}">${r.name}</option>`);
    res.send(options);
  });
});

app.get('/api/subregions', (req, res) => {
  // 使用 GROUP BY 去除重複的 sub-region 名稱
  pool.query('SELECT MIN(id) as id, name FROM sub_regions GROUP BY name ORDER BY name ASC', (err, results) => {
    if (err) {
      console.error('❌ /api/subregions error:', err.message);
      return res.send('<option>Error loading data</option>');
    }
    if (!results || results.length === 0) {
      console.warn('⚠️ No sub-regions found in database');
      return res.send('<option>No sub-regions found</option>');
    }
    let options = '<option value="" selected>Select Sub-Region</option>';
    results.forEach(r => options += `<option value="${r.id}">${r.name}</option>`);
    res.send(options);
  });
});

app.get('/api/decades', (req, res) => {
  pool.query('SELECT DISTINCT year FROM dadd_records ORDER BY year ASC', (err, results) => {
    if (err) {
      console.error('❌ /api/decades error:', err.message);
      return res.send('<option>Error loading data</option>');
    }
    if (!results || results.length === 0) {
      console.warn('⚠️ No decades found in database');
      return res.send('<option>No decades found</option>');
    }
    let options = '<option value="" selected>Select Decade</option>';
    results.forEach(r => options += `<option value="${r.year}">${r.year}s</option>`);
    res.send(options);
  });
});

// ==========================================
// 功能 API
// ==========================================
app.post('/feature1', (req, res) => {
  pool.query('SELECT year, amount FROM dadd_records WHERE country_code = ? ORDER BY year ASC', [req.body.country_code], (err, results) => {
    if (err || results.length === 0) return res.send('<div class="text-muted p-2">No data found.</div>');
    let html = '<table class="table table-sm"><thead><tr><th>Decade</th><th>Deaths</th></tr></thead><tbody>';
    results.forEach(r => html += `<tr><td>${r.year}s</td><td>${r.amount}</td></tr>`);
    res.send(html + '</tbody></table>');
  });
});

app.post('/feature2', (req, res) => {
  const sql = `SELECT c.name, d.amount FROM dadd_records d JOIN countries c ON d.country_code = c.code WHERE c.subregion_id = ? AND d.year = ? ORDER BY d.amount DESC`;
  pool.query(sql, [req.body.subregion_id, req.body.decade], (err, results) => {
    if (err) return res.send('<div class="text-danger p-2">Error or No Data</div>');
    let html = '<table class="table table-sm"><thead><tr><th>Country</th><th>Deaths</th></tr></thead><tbody>';
    results.forEach(r => html += `<tr><td>${r.name}</td><td>${r.amount}</td></tr>`);
    res.send(html + '</tbody></table>');
  });
});

app.post('/feature3', (req, res) => {
  const sql = `SELECT s.name as sub_name, MAX(d.amount) as max_val FROM dadd_records d JOIN countries c ON d.country_code = c.code JOIN sub_regions s ON c.subregion_id = s.id WHERE s.region_id = ? AND d.year = ? GROUP BY s.id, s.name ORDER BY max_val DESC`;
  pool.query(sql, [req.body.region_id, req.body.decade], (err, results) => {
    if (err) return res.send('<div class="text-danger p-2">Error or No Data</div>');
    let html = '<table class="table table-sm"><thead><tr><th>Sub-Region</th><th>Max DADD</th></tr></thead><tbody>';
    results.forEach(r => html += `<tr><td>${r.sub_name}</td><td>${r.max_val}</td></tr>`);
    res.send(html + '</tbody></table>');
  });
});

app.post('/feature4', (req, res) => {
  const keyword = `%${req.body.keyword}%`;
  const sql = `SELECT c.name, d.year, d.amount FROM countries c JOIN dadd_records d ON c.code = d.country_code WHERE c.name LIKE ? AND d.year = (SELECT MAX(year) FROM dadd_records WHERE country_code = c.code) ORDER BY d.amount DESC`;
  pool.query(sql, [keyword], (err, results) => {
    if (err) return res.send('<div class="text-muted p-2">Searching...</div>');
    if (results.length === 0) return res.send('<div class="text-muted p-2">No matches.</div>');
    let html = '<table class="table table-sm"><thead><tr><th>Country</th><th>Decade</th><th>Deaths</th></tr></thead><tbody>';
    results.forEach(r => html += `<tr><td>${r.name}</td><td>${r.year}s</td><td><strong>${r.amount}</strong></td></tr>`);
    res.send(html + '</tbody></table>');
  });
});

// Feature 5: Add new DADD record for next decade
app.post('/feature5', (req, res) => {
  const { country_code, amount } = req.body;
  
  // Get the latest decade for this country
  const getMaxYearSql = 'SELECT MAX(year) as max_year FROM dadd_records WHERE country_code = ?';
  pool.query(getMaxYearSql, [country_code], (err, results) => {
    if (err) {
      console.error('Error getting max year:', err);
      return res.send('<div class="alert alert-danger">Error retrieving data</div>');
    }
    
    const nextDecade = results[0].max_year ? results[0].max_year + 10 : 2020;
    
    // Insert new record
    const insertSql = 'INSERT INTO dadd_records (country_code, year, amount) VALUES (?, ?, ?)';
    pool.query(insertSql, [country_code, nextDecade, amount], (err2, result) => {
      if (err2) {
        console.error('Error inserting record:', err2);
        return res.send('<div class="alert alert-danger">Error adding record</div>');
      }
      res.send(`<div class="alert alert-success">✓ Successfully added record for ${nextDecade}s with value ${amount}</div>`);
    });
  });
});

// Feature 6: Update existing DADD record
app.post('/feature6', (req, res) => {
  const { country_code, decade, amount } = req.body;
  
  const sql = 'UPDATE dadd_records SET amount = ? WHERE country_code = ? AND year = ?';
  pool.query(sql, [amount, country_code, decade], (err, result) => {
    if (err) {
      console.error('Error updating record:', err);
      return res.send('<div class="alert alert-danger">Error updating record</div>');
    }
    if (result.affectedRows === 0) {
      return res.send('<div class="alert alert-warning">No record found to update</div>');
    }
    res.send(`<div class="alert alert-success">✓ Successfully updated ${result.affectedRows} record(s)</div>`);
  });
});

// Feature 7: Delete DADD records in decade range
app.post('/feature7', (req, res) => {
  const { country_code, start_decade, end_decade } = req.body;
  
  const sql = 'DELETE FROM dadd_records WHERE country_code = ? AND year >= ? AND year <= ?';
  pool.query(sql, [country_code, start_decade, end_decade], (err, result) => {
    if (err) {
      console.error('Error deleting records:', err);
      return res.send('<div class="alert alert-danger">Error deleting records</div>');
    }
    res.send(`<div class="alert alert-success">✓ Successfully deleted ${result.affectedRows} record(s)</div>`);
  });
});

// Feature 8: Compare regions - show average DADD by region for a decade
app.post('/feature8', (req, res) => {
  const { decade } = req.body;
  
  const sql = `
    SELECT r.name as region_name, 
           AVG(d.amount) as avg_dadd,
           COUNT(DISTINCT c.code) as country_count
    FROM dadd_records d
    JOIN countries c ON d.country_code = c.code
    JOIN sub_regions s ON c.subregion_id = s.id
    JOIN regions r ON s.region_id = r.id
    WHERE d.year = ?
    GROUP BY r.id, r.name
    ORDER BY avg_dadd DESC
  `;
  
  pool.query(sql, [decade], (err, results) => {
    if (err) {
      console.error('Error comparing regions:', err);
      return res.send('<div class="alert alert-danger">Error retrieving data</div>');
    }
    if (results.length === 0) {
      return res.send('<div class="text-muted p-2">No data found for this decade.</div>');
    }
    
    let html = '<table class="table"><thead><tr><th>Region</th><th>Avg DADD</th><th>Countries</th></tr></thead><tbody>';
    results.forEach(r => {
      html += `<tr><td><strong>${r.region_name}</strong></td><td>${r.avg_dadd.toFixed(1)}</td><td>${r.country_count}</td></tr>`;
    });
    html += '</tbody></table>';
    res.send(html);
  });
});

app.listen(port, () => {
  console.log(`⚡ Server started on port ${port}`);
  // 給容器網路一點時間穩定後再嘗試連線
  setTimeout(() => testConnection(), 2000);
});