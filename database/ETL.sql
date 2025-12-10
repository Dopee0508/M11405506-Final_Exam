-- DADD Exam ETL Script (Final Clean Version)

CREATE DATABASE IF NOT EXISTS dadd_db;
USE dadd_db;

SET SESSION sql_mode = '';
SET FOREIGN_KEY_CHECKS = 0;

-- 1. 清理舊表
DROP TABLE IF EXISTS dadd_records;
DROP TABLE IF EXISTS countries;
DROP TABLE IF EXISTS sub_regions;
DROP TABLE IF EXISTS regions;

-- 2. 建立表格
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

-- 3. 暫存表 (修正欄位以符合實際 CSV 格式)
DROP TEMPORARY TABLE IF EXISTS temp_facts;
CREATE TEMPORARY TABLE temp_facts (
    Country_name VARCHAR(255), Year INT, DADD VARCHAR(50)
);

DROP TEMPORARY TABLE IF EXISTS temp_nations;
CREATE TEMPORARY TABLE temp_nations (
    name VARCHAR(255), alpha_2 VARCHAR(50), alpha_3 VARCHAR(50),
    country_code INT, iso_3166_2 VARCHAR(50), region VARCHAR(255), 
    sub_region VARCHAR(255), intermediate_region VARCHAR(255), 
    region_code VARCHAR(50), sub_region_code VARCHAR(50), 
    intermediate_region_code VARCHAR(50)
);

-- 4. 載入資料 (修正以符合實際 CSV 格式)
LOAD DATA INFILE '/var/lib/mysql-files/data1.csv'
INTO TABLE temp_facts
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Country_name, Year, DADD);

LOAD DATA INFILE '/var/lib/mysql-files/data2.csv'
INTO TABLE temp_nations
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(name, alpha_2, alpha_3, country_code, iso_3166_2, region, sub_region, 
 intermediate_region, region_code, sub_region_code, intermediate_region_code);

-- 5. 轉換與清理 (使用 REPLACE 去除 \r 和 \n)

-- Load Regions
INSERT IGNORE INTO regions (name)
SELECT DISTINCT TRIM(REPLACE(REPLACE(region, '\r', ''), '\n', ''))
FROM temp_nations 
WHERE region IS NOT NULL AND region != '';

-- Load SubRegions
INSERT IGNORE INTO sub_regions (name, region_id)
SELECT DISTINCT TRIM(REPLACE(REPLACE(n.sub_region, '\r', ''), '\n', '')), r.id
FROM temp_nations n
JOIN regions r ON TRIM(REPLACE(REPLACE(n.region, '\r', ''), '\n', '')) = r.name
WHERE n.sub_region IS NOT NULL AND n.sub_region != '';

-- Load Countries
INSERT IGNORE INTO countries (code, name, code2, subregion_id)
SELECT DISTINCT TRIM(REPLACE(n.alpha_3, '\r', '')), TRIM(n.name), TRIM(n.alpha_2), s.id
FROM temp_nations n
JOIN regions r ON TRIM(REPLACE(REPLACE(n.region, '\r', ''), '\n', '')) = r.name
JOIN sub_regions s ON TRIM(REPLACE(REPLACE(n.sub_region, '\r', ''), '\n', '')) = s.name AND s.region_id = r.id
WHERE n.alpha_3 IS NOT NULL AND CHAR_LENGTH(TRIM(n.alpha_3)) = 3;

-- Load Records (透過國家名稱關聯,因為 CSV 沒有 Code 欄位)
INSERT INTO dadd_records (country_code, year, amount)
SELECT c.code, f.Year, CAST(f.DADD AS FLOAT)
FROM temp_facts f
JOIN countries c ON TRIM(f.Country_name) = TRIM(c.name)
WHERE f.DADD IS NOT NULL AND f.DADD != '' AND f.DADD REGEXP '^[0-9.]+$';