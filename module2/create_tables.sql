CREATE DATABASE IF NOT EXISTS ai;

USE ai;

CREATE TABLE IF NOT EXISTS cities
(
    country    STRING,
    city       STRING,
    accentcity STRING,
    region     STRING,
    population INT,
    latitude   DECIMAL(8, 6),
    longitude  DECIMAL(8, 6)
);

CREATE TABLE IF NOT EXISTS nobel_laureates
(
    year                 SMALLINT,
    category             STRING,
    prize                STRING,
    motivation           STRING,
    prize_share          STRING,
    laureate_id          SMALLINT,
    laureate_type        STRING,
    full_name            STRING,
    birth_date           DATE,
    birth_city           STRING,
    birth_country        STRING,
    sex                  STRING,
    organization_name    STRING,
    organization_city    STRING,
    organization_country STRING,
    death_date           DATE,
    death_city           STRING,
    death_country        STRING
);

CREATE TABLE IF NOT EXISTS countries
(
    country          STRING,
    region           STRING,
    population       INT,
    area             INT,
    pop_density      STRING,
    coastline        STRING,
    net_migration    STRING,
    infant_mortality STRING,
    gdp              INT,
    literacy         STRING,
    phones           STRING,
    arable           STRING,
    crops            STRING,
    other            STRING,
    climate          TINYINT,
    birthrate        STRING,
    deathrate        STRING,
    agriculture      STRING,
    industry         STRING,
    service          STRING
);

CREATE TABLE IF NOT EXISTS continents
(
    country_code   STRING,
    continent_code STRING
);

CREATE TABLE IF NOT EXISTS currencies
(
    country_code  STRING,
    currency_code STRING
);

CREATE TABLE IF NOT EXISTS iso3
(
    iso2_country_code STRING,
    iso3_country_code STRING
);

CREATE TABLE IF NOT EXISTS country_names
(
    country_code STRING,
    country_name STRING
);

CREATE TABLE IF NOT EXISTS capitals
(
    country_code STRING,
    capital      STRING
);

CREATE TABLE IF NOT EXISTS phones
(
    country_code STRING,
    phone_code   STRING
);



