CREATE TABLE ds_laureates
(
    award_year                 smallint     NOT NULL,
    award_category             varchar(20)  NOT NULL,
    laureate_id                smallint     NOT NULL,
    laureate_type              varchar(20)  NOT NULL,
    laureate_full_name         varchar(100) NOT NULL,
    laureate_gender            varchar(6),
    laureate_birth_date        date,
    laureate_birth_city        varchar(50),
    city_population            integer,
    laureate_birth_country     varchar(50),
    country_area               integer,
    country_climate            varchar(300), -- https://www.kaggle.com/fernandol/countries-of-the-world/discussion/58531
    country_population         integer,
    country_population_density decimal(5, 1),
    country_net_migration      decimal(4, 2),
    country_infant_mortality   decimal(5, 2),
    country_birthrate          decimal(4, 2),
    country_deathrate          decimal(4, 2),
    country_literacy           decimal(4, 1),
    country_gdp                integer,
    country_gdp_agriculture    decimal(4, 3),
    country_gdp_industry       decimal(4, 3),
    country_gdp_service        decimal(4, 3)
);

