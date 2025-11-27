--creaing dimension tables for the countries
CREATE TABLE IF NOT EXISTS dim_countries (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(255) NOT NULL UNIQUE,
    iso_code VARCHAR(20),
    population BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--now creating fact table for the daily stats
CREATE TABLE IF NOT EXISTS fact_covid_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    country_id INT REFERENCES dim_countries(country_id),
    cases INT,
    deaths INT,
    recovered INT,
    tests INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, country_id)  -- Prevent duplicate entries for same country on same day
);
