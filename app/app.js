const express = require('express');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const path = require('path');
const app = express();
const port = 3000;

app.use(bodyParser.urlencoded({ extended: true }));

const pool = mysql.createPool({
  host: process.env.DB_HOST || 'db',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || 'root',
  database: process.env.DB_NAME || 'dadd_db',
  port: process.env.DB_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  connectTimeout: 20000
});

// 測試資料庫連線
pool.getConnection((err, connection) => {
  if (err) {
    console.error('❌ Database connection failed:', err.message);
  } else {
    console.log('✅ Database connected successfully!');
    connection.release();
  }
});

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
    console.log(`✅ Loaded ${results.length} countries`);
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
    console.log(`✅ Loaded ${results.length} regions`);
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
    console.log(`✅ Loaded ${results.length} sub-regions`);
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
    console.log(`✅ Loaded ${results.length} decades`);
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

app.listen(port, () => {
  console.log(`App running on port ${port}`);
});