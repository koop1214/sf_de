-------------------------------------------------- STAGING -------------------------------------------------------------
CREATE TABLE undkit_staging5.countries
(
    country          STRING,
    region           STRING,
    population       INT,
    area             INT,
    pop_density      DECIMAL(5, 1),
    coastline        DECIMAL(5, 2),
    net_migration    DECIMAL(4, 2),
    infant_mortality DECIMAL(5, 2),
    gdp              INT,
    literacy         DECIMAL(4, 1),
    phones           DECIMAL(5, 1),
    arable           DECIMAL(4, 2),
    crops            DECIMAL(4, 2),
    other            DECIMAL(5, 2),
    climate          TINYINT,
    birthrate        DECIMAL(5, 2),
    deathrate        DECIMAL(5, 2),
    agriculture      DECIMAL(4, 3),
    industry         DECIMAL(4, 3),
    service          DECIMAL(4, 3)
)
PARTITIONED BY
(
    updated_at date
)
STORED AS ORC;

CREATE TABLE undkit_staging5.cities
(
    country    STRING,
    city       STRING,
    accentcity STRING,
    region     STRING,
    population DOUBLE,
    latitude   DOUBLE,
    longitude  DOUBLE
)
PARTITIONED BY
(
    updated_at date
)
STORED AS ORC;


CREATE TABLE undkit_staging5.nobel_laureates
(
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
)
PARTITIONED BY
(
    imported_at date
)
STORED AS ORC;

-- остальные не меняются

---------------------------------------------------- SNOWFLAKE ---------------------------------------------------------
CREATE TABLE undkit_dwh5.countries
(
    id                 SMALLINT PRIMARY KEY,
    code               STRING NOT NULL,
    name               STRING NOT NULL,
    area               INT,
    climate_id         SMALLINT,
    population         INT,
    population_density DECIMAL(5, 1),
    net_migration      DECIMAL(4, 2),
    infant_mortality   DECIMAL(5, 2),
    birthrate          DECIMAL(4, 2),
    deathrate          DECIMAL(4, 2),
    literacy           DECIMAL(4, 1),
    gdp                INT,
    gdp_agriculture    DECIMAL(4, 3),
    gdp_industry       DECIMAL(4, 3),
    gdp_service        DECIMAL(4, 3)
) STORED AS ORC;

CREATE TABLE undkit_dwh5.cities
(
    id         SMALLINT PRIMARY KEY,
    name       STRING   NOT NULL,
    country_id SMALLINT NOT NULL,
    population INT
)
CLUSTERED BY (country_id) into 20 buckets
STORED AS ORC;


CREATE TABLE undkit_dwh5.nobel_prizes
(
    prize_year             SMALLINT,
    category_id            SMALLINT,
    id                     SMALLINT PRIMARY KEY,
    laureate_type_id       SMALLINT NOT NULL,
    laureate_full_name     STRING   NOT NULL,
    laureate_gender_id     SMALLINT,
    laureate_birth_date    DATE,
    laureate_birth_city_id SMALLINT
)
STORED AS ORC;

-- остальные не меняются

---------------------------------------------------- DATASET -----------------------------------------------------------
CREATE TABLE undkit_dwh5.ds_laureates
(
    laureate_id                      SMALLINT NOT NULL,
    laureate_type                    STRING   NOT NULL,
    laureate_full_name               STRING   NOT NULL,
    laureate_gender                  STRING,
    laureate_birth_date              DATE,
    laureate_birth_city              STRING,
    laureate_birth_country           STRING,
    birth_country_area               INT,
    birth_country_population         INT,
    birth_country_population_density DECIMAL(5, 1),
    birth_country_net_migration      DECIMAL(4, 2),
    birth_country_infant_mortality   DECIMAL(5, 2),
    birth_country_birthrate          DECIMAL(5, 2),
    birth_country_deathrate          DECIMAL(5, 2),
    birth_country_literacy           DECIMAL(4, 1),
    birth_country_gdp                INT,
    birth_country_gdp_agriculture    DECIMAL(4, 3),
    birth_country_gdp_industry       DECIMAL(4, 3),
    birth_country_gdp_service        DECIMAL(4, 3),
    birth_country_climate            STRING,
    birth_city_population            INT
)
PARTITIONED BY
(
    prize_year                       SMALLINT,
    prize_category                   STRING
)
STORED AS ORC;







