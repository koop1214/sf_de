from pyspark import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# SNOWFLAKE
# =========
# climates
# ----------------------
# CREATE TABLE climates
# (
#     id          smallint PRIMARY KEY,
#     description varchar(300) NOT NULL
# );

climatesDF = spark.createDataFrame([
    (1, 'Dry tropical or tundra and ice, classification B and E'),
    (2, 'Wet tropical, classification A'),
    (3, 'Temperate humid subtropical and temperate continental, classification Cfa, Cwa, and D'),
    (4, 'Dry hot summers and wet winters')]) \
    .toDF("id", "description")

climatesDF.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .saveAsTable("undkit_dwh.climates")

# countries
# ----------------------
# CREATE TABLE countries
# (
#     id                 smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
#     code               varchar(2) NOT NULL UNIQUE,
#     name               varchar(20) NOT NULL,
#     area               integer,
#     climate_id         smallint   NOT NULL REFERENCES climates (id),
#     population         integer,
#     population_density decimal(5, 1),
#     net_migration      decimal(4, 2),
#     infant_mortality   decimal(5, 2),
#     birthrate          decimal(4, 2),
#     deathrate          decimal(4, 2),
#     literacy           decimal(4, 1),
#     gdp                integer,
#     gdp_agriculture    decimal(4, 3),
#     gdp_industry       decimal(4, 3),
#     gdp_service        decimal(4, 3)
# );

countriesDF = spark.sql("""
SELECT 
        REPLACE(
               REPLACE(
                       REPLACE(country, '&', 'and')
                   , ', The', '')
           , 'Is.', 'Islands') AS name,
       area,
       climate     AS climate_id,
       population,
       pop_density AS population_density,
       net_migration,
       infant_mortality,
       birthrate,
       deathrate,
       literacy,
       gdp,
       agriculture AS gdp_agriculture,
       industry    AS gdp_industry,
       service     AS gdp_service
  FROM undkit_staging.countries_orc
""") \
    .withColumn('id', f.row_number().over(Window.orderBy("name")))

countryNamesDF = spark.sql('SELECT * FROM undkit_staging.country_names_orc')

countriesDF = countriesDF.join(countryNamesDF, countriesDF["name"] == countryNamesDF['country_name'], "left_outer") \
    .drop('country_name') \
    .withColumnRenamed('country_code', 'code')

countriesDF = countriesDF.select(countriesDF.columns[-2:] + countriesDF.columns[:-2])

hiveContext = HiveContext(spark.sparkContext)
hiveContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

countriesDF.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .saveAsTable("undkit_dwh.countries")

# cities
# -------
# CREATE TABLE cities
# (
#     id         smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
#     name       varchar(20) NOT NULL,
#     country_id smallint    NOT NULL REFERENCES countries (id),
#     population integer
# );

citiesDF = spark.sql("""
SELECT INITCAP(TRIM(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(city, '"', ''), '^[\\'-]', ''), '`', ''), '((', ''), '))', '')))        AS city_name,
       UPPER(country)                     AS country_code,
       NULLIF(CAST(population AS INT), 0) AS city_population
  FROM undkit_staging.cities_orc
 WHERE city != ''
""")

citiesDF = citiesDF.withColumn('rn', f.row_number().over(
    Window.partitionBy(['city_name', 'country_code']).orderBy(f.desc("city_population")))) \
    .filter("rn = 1") \
    .withColumn('city_id', f.row_number().over(Window.orderBy("city_name"))) \
    .drop('rn')

citiesDF = citiesDF.join(countriesDF, citiesDF["country_code"] == countriesDF['code'], "left_outer") \
    .selectExpr('city_id as id', 'city_name as name', 'id as country_id', 'city_population as population')

citiesDF.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .saveAsTable("undkit_dwh.cities")

# prize_categories
# -----------------
# CREATE TABLE prize_categories
# (
#     id   smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
#     name varchar(20) NOT NULL UNIQUE
# );

laureatsDF = spark.sql("""
SELECT year                      AS prize_year,
       category                  AS prize_category,
       laureate_id,
       laureate_type,
       full_name                 AS laureate_full_name,
       NULLIF(sex, '')           AS laureate_gender,
       birth_date                AS laureate_birth_date,
       NULLIF(birth_city, '')    AS laureate_birth_city,
       NULLIF(birth_country, '') AS laureate_birth_country
  FROM undkit_staging.nobel_laureates_orc
""").distinct()

categoriesDF = laureatsDF.select('prize_category') \
    .distinct() \
    .withColumn('id', f.row_number().over(Window.orderBy("prize_category"))) \
    .selectExpr('id', 'prize_category as name')

categoriesDF.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .saveAsTable("undkit_dwh.prize_categories")

# laureate_types
# --------------
"""
CREATE TABLE laureate_types
(
    id   smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name varchar(20) NOT NULL UNIQUE
);
"""

typesDF = laureatsDF.select('laureate_type') \
    .distinct() \
    .withColumn('id', f.row_number().over(Window.orderBy("laureate_type"))) \
    .selectExpr('id', 'laureate_type as name')

typesDF.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .saveAsTable("undkit_dwh.laureate_types")

# genders
# -------------
# CREATE TABLE genders
# (
#     id   smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
#     name varchar(6) NOT NULL UNIQUE
# );

gendersDF = laureatsDF.select('laureate_gender') \
    .distinct() \
    .filter("laureate_gender != ''") \
    .withColumn('id', f.row_number().over(Window.orderBy("laureate_gender"))) \
    .selectExpr('id', 'laureate_gender as name')

gendersDF.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .saveAsTable("undkit_dwh.genders")

# nobel_prizes
# -------------
# CREATE TABLE nobel_prizes
# (
#     id                     smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
#     prize_year             smallint     NOT NULL,
#     category_id            smallint     NOT NULL REFERENCES prize_categories (id),
#     laureate_type_id       smallint     NOT NULL REFERENCES laureate_types (id),
#     laureate_full_name     varchar(100) NOT NULL,
#     laureate_gender_id     smallint REFERENCES genders (id),
#     laureate_birth_date    date,
#     laureate_birth_city_id smallint REFERENCES cities (id)
# );

nobelsDF = laureatsDF.drop('laureate_id')

nobelsDF = nobelsDF.join(categoriesDF, categoriesDF["name"] == nobelsDF['prize_category'], "left_outer") \
    .drop('name', 'prize_category') \
    .withColumnRenamed('id', 'category_id')

nobelsDF = nobelsDF.join(typesDF, typesDF["name"] == nobelsDF['laureate_type'], "left_outer") \
    .drop('name', 'laureate_type') \
    .withColumnRenamed('id', 'laureate_type_id')

nobelsDF = nobelsDF.join(gendersDF, gendersDF["name"] == nobelsDF['laureate_gender'], "left_outer") \
    .drop('name', 'laureate_gender') \
    .withColumnRenamed('id', 'laureate_gender_id')

nobelsDF = nobelsDF.join(countriesDF, countriesDF["name"] == nobelsDF['laureate_birth_country'], "left_outer") \
    .drop('laureate_birth_country', 'code', 'name', 'area', 'climate_id', 'population', 'population_density',
          'net_migration', 'infant_mortality', 'birthrate', 'deathrate', 'literacy', 'gdp', 'gdp_agriculture',
          'gdp_industry', 'gdp_service') \
    .withColumnRenamed('id', 'laureate_birth_country_id')

nobelsDF = nobelsDF.join(citiesDF, (citiesDF["name"] == nobelsDF['laureate_birth_city']) & (
            citiesDF["country_id"] == nobelsDF['laureate_birth_country_id']), "left_outer") \
    .drop('laureate_birth_city', 'laureate_birth_country_id', 'name', 'country_id', 'population', 'laureate_gender') \
    .withColumnRenamed('id', 'laureate_birth_city_id')

nobelsDF = nobelsDF.withColumn('id', f.row_number().over(Window.orderBy("prize_year"))) \
    .select('id', 'prize_year', 'category_id', 'laureate_type_id', 'laureate_full_name', 'laureate_gender_id',
            'laureate_birth_date', 'laureate_birth_city_id')

nobelsDF.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .saveAsTable("undkit_dwh.nobel_prizes")

# DATASET
# =========
# CREATE TABLE ds_laureates
# (
#     prize_year                       smallint     NOT NULL,
#     prize_category                   varchar(20)  NOT NULL,
#     laureate_id                      smallint     NOT NULL,
#     laureate_type                    varchar(20)  NOT NULL,
#     laureate_full_name               varchar(100) NOT NULL,
#     laureate_gender                  varchar(6),
#     laureate_birth_date              date,
#     laureate_birth_city              varchar(50),
#     birth_city_population            integer,
#     laureate_birth_country           varchar(50),
#     birth_country_area               integer,
#     birth_country_climate            varchar(300), -- https://www.kaggle.com/fernandol/countries-of-the-world/discussion/58531
#     birth_country_population         integer,
#     birth_country_population_density decimal(5, 1),
#     birth_country_net_migration      decimal(4, 2),
#     birth_country_infant_mortality   decimal(5, 2),
#     birth_country_birthrate          decimal(4, 2),
#     birth_country_deathrate          decimal(4, 2),
#     birth_country_literacy           decimal(4, 1),
#     birth_country_gdp                integer,
#     birth_country_gdp_agriculture    decimal(4, 3),
#     birth_country_gdp_industry       decimal(4, 3),
#     birth_country_gdp_service        decimal(4, 3)
# );

countriesDFbirth = countriesDF.select([f.col(c).alias("birth_country_" + c) for c in countriesDF.columns])

datasetDF = laureatsDF.join(countriesDFbirth, countriesDFbirth["birth_country_name"] == laureatsDF['laureate_birth_country'], "left_outer") \
    .drop('birth_country_name', 'birth_country_code')

datasetDF = datasetDF.join(climatesDF, climatesDF["id"] == datasetDF['birth_country_climate_id'], "left_outer") \
    .drop('birth_country_climate_id', 'id') \
    .withColumnRenamed('description', 'birth_country_climate')

datasetDF = datasetDF.join(citiesDF, (citiesDF["name"] == datasetDF['laureate_birth_city']) & (citiesDF["country_id"] == datasetDF['birth_country_id']), "left_outer") \
    .drop('id', 'name', 'country_id', 'birth_country_id') \
    .withColumnRenamed('population', 'birth_city_population')

datasetDF.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .saveAsTable("undkit_dwh.ds_laureates")
