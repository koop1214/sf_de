CREATE TABLE prize_categories
(
    id   smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name varchar(20) NOT NULL UNIQUE
);

CREATE TABLE laureate_types
(
    id   smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name varchar(20) NOT NULL UNIQUE
);

CREATE TABLE genders
(
    id   smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name varchar(6) NOT NULL UNIQUE
);

CREATE TABLE climates
(
    id          smallint PRIMARY KEY,
    description varchar(300) NOT NULL
);

CREATE TABLE countries
(
    id                 smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    code               varchar(2) NOT NULL UNIQUE,
    area               integer,
    climate_id         smallint   NOT NULL REFERENCES climates (id),
    population         integer,
    population_density decimal(5, 1),
    net_migration      decimal(4, 2),
    infant_mortality   decimal(5, 2),
    birthrate          decimal(4, 2),
    deathrate          decimal(4, 2),
    literacy           decimal(4, 1),
    gdp                integer,
    gdp_agriculture    decimal(4, 3),
    gdp_industry       decimal(4, 3),
    gdp_service        decimal(4, 3)
);

CREATE TABLE cities
(
    id         smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name       varchar(20) NOT NULL,
    country_id smallint    NOT NULL REFERENCES countries (id),
    population integer
);

CREATE TABLE nobel_prizes
(
    id                     smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    prize_year             smallint     NOT NULL,
    category_id            smallint     NOT NULL REFERENCES prize_categories (id),
    laureate_type_id       smallint     NOT NULL REFERENCES laureate_types (id),
    laureate_full_name     varchar(100) NOT NULL,
    laureate_gender_id     smallint REFERENCES genders (id),
    laureate_birth_date    date,
    laureate_birth_city_id smallint REFERENCES cities (id)
);

