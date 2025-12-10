-- DADD Exam ETL Script (Final Windows Fix)

CREATE DATABASE IF NOT EXISTS dadd_db;
USE dadd_db;

-- 寬容模式，避免因為 CSV 格式小問題而報錯停止
SET SESSION sql_mode = '';
SET FOREIGN_KEY_CHECKS = 0;

-- 1. 清理舊表
DROP TABLE IF EXISTS dadd_records;
DROP TABLE IF EXISTS countries;
DROP TABLE IF EXISTS sub_regions;
DROP TABLE IF EXISTS regions;

-- 2. 建立正式表格 (3NF)
CREATE TABLE regions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE sub_regions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    region_id INT,
    FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE CASCADE,
    UNIQUE KEY unique_sub_region (name, region_id)
);

CREATE TABLE countries (
    code CHAR(3) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    code2 CHAR(2),
    subregion_id INT,
    FOREIGN KEY (subregion_id) REFERENCES sub_regions(id) ON DELETE SET NULL
);

CREATE TABLE dadd_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    country_code CHAR(3),
    year INT NOT NULL,
    amount FLOAT DEFAULT 0,
    FOREIGN KEY (country_code) REFERENCES countries(code) ON DELETE CASCADE
);

SET FOREIGN_KEY_CHECKS = 1;

-- 3. 建立暫存表 (Staging)
DROP TEMPORARY TABLE IF EXISTS temp_facts;
CREATE TEMPORARY TABLE temp_facts (
    Entity VARCHAR(255),
    Code VARCHAR(50),
    Year INT,
    DADD VARCHAR(50) -- 先用文字接，避免浮點數轉換錯誤
);

DROP TEMPORARY TABLE IF EXISTS temp_nations;
CREATE TEMPORARY TABLE temp_nations (
    name VARCHAR(255),
    alpha_2 VARCHAR(50),
    alpha_3 VARCHAR(50),
    country_code INT,
    region VARCHAR(255),
    sub_region VARCHAR(255),
    intermediate_region VARCHAR(255),
    region_code VARCHAR(50),
    sub_region_code VARCHAR(50),
    intermediate_region_code VARCHAR(50)
);

-- 4. 載入 CSV (針對 Windows 換行符號最佳化)
LOAD DATA INFILE '/var/lib/mysql-files/data1.csv'
INTO TABLE temp_facts
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/var/lib/mysql-files/data2.csv'
INTO TABLE temp_nations
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

-- 5. 執行 ETL 轉換 (使用 TRIM 去除隱藏空白)

-- 5.1 Regions
INSERT IGNORE INTO regions (name)
SELECT DISTINCT TRIM(region)
FROM temp_nations 
WHERE region IS NOT NULL AND region != '';

-- 5.2 SubRegions
INSERT IGNORE INTO sub_regions (name, region_id)
SELECT DISTINCT TRIM(n.sub_region), r.id
FROM temp_nations n
JOIN regions r ON TRIM(n.region) = r.name
WHERE n.sub_region IS NOT NULL AND n.sub_region != '';

-- 5.3 Countries
INSERT IGNORE INTO countries (code, name, code2, subregion_id)
SELECT DISTINCT TRIM(n.alpha_3), TRIM(n.name), TRIM(n.alpha_2), s.id
FROM temp_nations n
JOIN regions r ON TRIM(n.region) = r.name
JOIN sub_regions s ON TRIM(n.sub_region) = s.name AND s.region_id = r.id
WHERE n.alpha_3 IS NOT NULL AND CHAR_LENGTH(TRIM(n.alpha_3)) = 3;

-- 5.4 DADD Records
INSERT INTO dadd_records (country_code, year, amount)
SELECT TRIM(f.Code), f.Year, CAST(f.DADD AS FLOAT)
FROM temp_facts f
JOIN countries c ON TRIM(f.Code) = c.code
WHERE f.DADD IS NOT NULL AND f.DADD != '';