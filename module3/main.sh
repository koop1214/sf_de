#!/bin/bash

path="$( cd "$( dirname $0 )" && cd -P "$( dirname "$SOURCE" )" && pwd )"
cd $path

dbName=undkit_staging

#################################################### Phones ############################################################
fileName="/tmp/ai/phone.csv"
tabName=phones
createFields="country_code STRING, phone_code STRING"
selectFields="*"

./createExternalTable.sh $fileName $tabName $dbName "$createFields" "$selectFields"

#################################################### Country names #####################################################
fileName="/tmp/ai/names.csv"
tabName=country_names
createFields="country_code STRING, country_name STRING"
selectFields="*"

./createExternalTable.sh $fileName $tabName $dbName "$createFields" "$selectFields"

#################################################### ISO3 ##############################################################
fileName="/tmp/ai/iso3.csv"
tabName=iso3
createFields="iso2_country_code STRING, iso3_country_code STRING"
selectFields="*"

./createExternalTable.sh $fileName $tabName $dbName "$createFields" "$selectFields"

#################################################### Currencies ########################################################
fileName="/tmp/ai/currency.csv"
tabName=currencies
createFields="country_code  STRING, currency_code STRING"
selectFields="*"

./createExternalTable.sh $fileName $tabName $dbName "$createFields" "$selectFields"

#################################################### Continents ########################################################
fileName="/tmp/ai/continent.csv"
tabName=continents
createFields="country_code  STRING, continent_code STRING"
selectFields="*"

./createExternalTable.sh $fileName $tabName $dbName "$createFields" "$selectFields"

###################################################### Capitals ########################################################
fileName="/tmp/ai/capital.csv"
tabName=capitals
createFields="country_code  STRING, capital STRING"
selectFields="*"

./createExternalTable.sh $fileName $tabName $dbName "$createFields" "$selectFields"

##################################################### Countries ########################################################
fileName="/home/deng/Data/countries_of_the_world.csv"
tabName=countries
createFields="country          STRING,
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
    service          DECIMAL(4, 3)"
selectFields="TRIM(country)                                       AS country,
       TRIM(region)                                               AS region,
       CAST(population AS INT),
       CAST(area AS INT),
       CAST(REPLACE(pop_density, ',', '.') AS DECIMAL(5, 1))      AS pop_density,
       CAST(REPLACE(coastline, ',', '.') AS DECIMAL(5, 2))        AS coastline,
       CAST(REPLACE(net_migration, ',', '.') AS DECIMAL(4, 2))    AS net_migration,
       CAST(REPLACE(infant_mortality, ',', '.') AS DECIMAL(5, 2)) AS infant_mortality,
       CAST(gdp AS INT),
       CAST(REPLACE(literacy, ',', '.') AS DECIMAL(4, 1))         AS literacy,
       CAST(REPLACE(phones, ',', '.') AS DECIMAL(5, 1))           AS phones,
       CAST(REPLACE(arable, ',', '.') AS DECIMAL(4, 2))           AS arable,
       CAST(REPLACE(crops, ',', '.') AS DECIMAL(4, 2))            AS crops,
       CAST(REPLACE(other, ',', '.') AS DECIMAL(5, 2))            AS other,
       CAST(climate AS TINYINT),
       CAST(REPLACE(birthrate, ',', '.') AS DECIMAL(5, 2))        AS birthrate,
       CAST(REPLACE(deathrate, ',', '.') AS DECIMAL(5, 2))        AS deathrate,
       CAST(REPLACE(agriculture, ',', '.') AS DECIMAL(4, 3))      AS agriculture,
       CAST(REPLACE(industry, ',', '.') AS DECIMAL(4, 3))         AS industry,
       CAST(REPLACE(service, ',', '.') AS DECIMAL(4, 3))          AS service"

./createExternalTable.sh $fileName $tabName $dbName "$createFields" "$selectFields"

##################################################### Nobel Laureates ##################################################
fileName="/home/deng/Data/nobel-laureates.csv"
tabName=nobel_laureates
createFields="year                 SMALLINT,
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
    death_country        STRING"
selectFields="CAST(year AS smallint),
       category,
       prize,
       motivation,
       prize_share,
       CAST(laureate_id AS smallint),
       laureate_type,
       full_name,
       CAST(birth_date AS date),
       birth_city,
       birth_country,
       sex,
       organization_name,
       organization_city,
       organization_country,
       CAST(death_date AS date),
       death_city,
       death_country"

./createExternalTable.sh $fileName $tabName $dbName "$createFields" "$selectFields"
