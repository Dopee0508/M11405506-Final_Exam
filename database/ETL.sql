-- DADD Exam ETL Script
-- This script handles Database Creation, Data Extraction, Transformation, and Loading.

-- ==========================================
-- 1. Database Schema Creation (DDL)
--    Design based on 3rd Normal Form (3NF)
-- ==========================================

-- Drop tables if they exist to ensure a clean start
DROP TABLE IF EXISTS DADD_Records CASCADE;
DROP TABLE IF EXISTS Countries CASCADE;
DROP TABLE IF EXISTS SubRegions CASCADE;
DROP TABLE IF EXISTS Regions CASCADE;

-- Table 1: Regions
-- Stores the main continents/regions (e.g., Asia, Africa)
CREATE TABLE Regions (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

-- Table 2: SubRegions
-- Stores sub-continents (e.g., Southern Asia), linked to Regions
CREATE TABLE SubRegions (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    region_id INTEGER REFERENCES Regions(id) ON DELETE CASCADE,
    UNIQUE(name, region_id) -- Prevent duplicates
);

-- Table 3: Countries
-- Stores country information, linked to SubRegions
-- Using ISO-3 code as the Primary Key as it is standard and unique
CREATE TABLE Countries (
    code CHAR(3) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    code2 CHAR(2),
    subregion_id INTEGER REFERENCES SubRegions(id) ON DELETE SET NULL
);

-- Table 4: DADD_Records
-- Stores the Decadal Average Annual Number of Deaths
-- This is the fact table linked to Countries
CREATE TABLE DADD_Records (
    id SERIAL PRIMARY KEY,
    country_code CHAR(3) REFERENCES Countries(code) ON DELETE CASCADE,
    year INTEGER NOT NULL, -- Represents the start of the decade (e.g., 1960)
    amount FLOAT DEFAULT 0
);

-- ==========================================
-- 2. Data Extraction (Staging)
--    Load raw CSV data into temporary tables
-- ==========================================

-- Create temporary table for data1.csv (Facts)
CREATE TEMP TABLE raw_facts (
    Entity VARCHAR(255),
    Code VARCHAR(10),
    Year INTEGER,
    DADD FLOAT
);

-- Create temporary table for data2.csv (Nations/Regions)
CREATE TEMP TABLE raw_nations (
    name VARCHAR(255),
    alpha_2 VARCHAR(2),
    alpha_3 VARCHAR(3),
    country_code INTEGER,
    region VARCHAR(255),
    sub_region VARCHAR(255),
    intermediate_region VARCHAR(255),
    region_code VARCHAR(50),
    sub_region_code VARCHAR(50),
    intermediate_region_code VARCHAR(50)
);

-- Load data from CSV files
-- NOTE: In Docker, we will mount the /database folder to /data inside the container
COPY raw_facts FROM '/data/data1.csv' DELIMITER ',' CSV HEADER;
COPY raw_nations FROM '/data/data2.csv' DELIMITER ',' CSV HEADER;

-- ==========================================
-- 3. Transformation and Loading (ETL)
--    Clean data and insert into 3NF tables
-- ==========================================

-- Step 3.1: Load Regions
-- Extract distinct regions from raw_nations
INSERT INTO Regions (name)
SELECT DISTINCT region 
FROM raw_nations 
WHERE region IS NOT NULL;

-- Step 3.2: Load SubRegions
-- Extract distinct sub-regions and link them to Region IDs
INSERT INTO SubRegions (name, region_id)
SELECT DISTINCT n.sub_region, r.id
FROM raw_nations n
JOIN Regions r ON n.region = r.name
WHERE n.sub_region IS NOT NULL;

-- Step 3.3: Load Countries
-- Extract countries and link them to SubRegion IDs
-- Filter: Only include valid ISO-3 codes
INSERT INTO Countries (code, name, code2, subregion_id)
SELECT DISTINCT n.alpha_3, n.name, n.alpha_2, s.id
FROM raw_nations n
JOIN Regions r ON n.region = r.name
JOIN SubRegions s ON n.sub_region = s.name AND s.region_id = r.id
WHERE n.alpha_3 IS NOT NULL;

-- Step 3.4: Load DADD Records
-- Transform logic:
-- 1. Join raw_facts with Countries to ensure referential integrity.
-- 2. Filter out erroneous data: 
--    - Rows where 'Code' in raw_facts is NULL or empty (often aggregate regions like "World").
--    - Rows that do not match any country in our Countries table.
INSERT INTO DADD_Records (country_code, year, amount)
SELECT f.Code, f.Year, f.DADD
FROM raw_facts f
JOIN Countries c ON f.Code = c.code -- This JOIN acts as a filter for valid countries only
WHERE f.Code IS NOT NULL 
  AND f.DADD IS NOT NULL;

-- Clean up
DROP TABLE raw_facts;
DROP TABLE raw_nations;

-- Verification (Optional comment for log)
-- SELECT count(*) FROM DADD_Records;