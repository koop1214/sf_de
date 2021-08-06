import sys

sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.10.7-src.zip')

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

import os, datetime
from pyspark.sql import SparkSession
from pyspark import HiveContext
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# настройки
DAG_NAME = 'ai_dwh'
DB_DWH = 'undkit_air_dwh'
DB_STG = 'undkit_air_staging'

# наш основной DAG
dag = DAG(dag_id=DAG_NAME, schedule_interval=None, start_date=datetime.datetime(2021, 8, 4), catchup=False)

# bash commands
exec_hive_command = dedent("""
/usr/bin/beeline -u jdbc:hive2://10.93.1.9:10000 -n hive -p cloudera -e "{{ params.sql }}"
""")

# OPERATORS

# init db
recreate_database = BashOperator(
    dag=dag,
    task_id='recreate_dwh_db',
    bash_command=exec_hive_command,
    params={"sql": """
        DROP DATABASE IF EXISTS {db} CASCADE;
        CREATE DATABASE {db};
    """.format(db=DB_DWH)},
)


def init_spark_session():
    os.environ["JAVA_HOME"] = "/usr"
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    os.environ["PYSPARK_SUBMIT_ARGS"] = """--driver-class-path /usr/share/java/mysql-connector-java.jar --jars /usr/share/java/mysql-connector-java.jar pyspark-shell"""

    spark = SparkSession.builder.master("yarn-client").appName("spark_airflow").config("hive.metastore.uris", "thrift://10.93.1.9:9083").enableHiveSupport().getOrCreate()

    hive_context = HiveContext(spark.sparkContext)
    hive_context.setConf("spark.sql.parquet.writeLegacyFormat", "true")

    return spark


def get_countries_df(spark: SparkSession):
    countries_df = spark.sql(f"""
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
      FROM {DB_STG}.countries_orc
    """) \
        .withColumn('id', f.row_number().over(Window.orderBy("name")))

    country_names_df = spark.sql(f'SELECT * FROM {DB_STG}.country_names_orc')

    countries_df = countries_df.join(country_names_df, countries_df["name"] == country_names_df['country_name'], "left_outer") \
        .drop('country_name') \
        .withColumnRenamed('country_code', 'code')

    countries_df = countries_df.select(countries_df.columns[-2:] + countries_df.columns[:-2])

    return countries_df


def get_cities_df(spark: SparkSession, countries_df=None):
    if countries_df is None:
        countries_df = get_countries_df(spark)

    cities_df = spark.sql(f"""
        SELECT INITCAP(TRIM(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(city, '"', ''), '^[\\'-]', ''), '`', ''), '((', ''), '))', '')))        AS city_name,
               UPPER(country)                     AS country_code,
               NULLIF(CAST(population AS INT), 0) AS city_population
          FROM {DB_STG}.cities_orc
         WHERE city != ''
        """)

    cities_df = cities_df.withColumn('rn', f.row_number().over(
        Window.partitionBy(['city_name', 'country_code']).orderBy(f.desc("city_population")))) \
        .filter("rn = 1") \
        .withColumn('city_id', f.row_number().over(Window.orderBy("city_name"))) \
        .drop('rn')

    cities_df = cities_df.join(countries_df, cities_df["country_code"] == countries_df['code'], "left_outer") \
        .selectExpr('city_id as id', 'city_name as name', 'id as country_id', 'city_population as population')

    return cities_df


def get_laureates_df(spark: SparkSession):
    return spark.sql(f"""
        SELECT year                      AS prize_year,
               category                  AS prize_category,
               laureate_id,
               laureate_type,
               full_name                 AS laureate_full_name,
               NULLIF(sex, '')           AS laureate_gender,
               birth_date                AS laureate_birth_date,
               NULLIF(birth_city, '')    AS laureate_birth_city,
               NULLIF(birth_country, '') AS laureate_birth_country
          FROM {DB_STG}.nobel_laureates_orc
        """).distinct()


def get_climates_df(spark: SparkSession):
    return spark.createDataFrame([
        (1, 'Dry tropical or tundra and ice, classification B and E'),
        (2, 'Wet tropical, classification A'),
        (3, 'Temperate humid subtropical and temperate continental, classification Cfa, Cwa, and D'),
        (4, 'Dry hot summers and wet winters')]) \
        .toDF("id", "description")


def create_snowflake_tables():
    spark = init_spark_session()

    # climates
    climates_df = get_climates_df(spark)

    climates_df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "gzip") \
        .saveAsTable(f"{DB_DWH}.climates")

    # countries
    countries_df = get_countries_df(spark)

    countries_df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "gzip") \
        .saveAsTable(f"{DB_DWH}.countries")

    # cities
    cities_df = get_cities_df(spark, countries_df)

    cities_df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "gzip") \
        .saveAsTable(f"{DB_DWH}.cities")

    laureates_df = get_laureates_df(spark)

    # prize_categories
    categories_df = laureates_df.select('prize_category') \
        .distinct() \
        .withColumn('id', f.row_number().over(Window.orderBy("prize_category"))) \
        .selectExpr('id', 'prize_category as name')

    categories_df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "gzip") \
        .saveAsTable(f"{DB_DWH}.prize_categories")

    # laureate_types
    types_df = laureates_df.select('laureate_type') \
        .distinct() \
        .withColumn('id', f.row_number().over(Window.orderBy("laureate_type"))) \
        .selectExpr('id', 'laureate_type as name')

    types_df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "gzip") \
        .saveAsTable(f"{DB_DWH}.laureate_types")

    # genders
    genders_df = laureates_df.select('laureate_gender') \
        .distinct() \
        .filter("laureate_gender != ''") \
        .withColumn('id', f.row_number().over(Window.orderBy("laureate_gender"))) \
        .selectExpr('id', 'laureate_gender as name')

    genders_df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "gzip") \
        .saveAsTable(f"{DB_DWH}.genders")

    # nobel_prizes
    nobels_df = laureates_df.drop('laureate_id')

    nobels_df = nobels_df.join(categories_df, categories_df["name"] == nobels_df['prize_category'], "left_outer") \
        .drop('name', 'prize_category') \
        .withColumnRenamed('id', 'category_id')

    nobels_df = nobels_df.join(types_df, types_df["name"] == nobels_df['laureate_type'], "left_outer") \
        .drop('name', 'laureate_type') \
        .withColumnRenamed('id', 'laureate_type_id')

    nobels_df = nobels_df.join(genders_df, genders_df["name"] == nobels_df['laureate_gender'], "left_outer") \
        .drop('name', 'laureate_gender') \
        .withColumnRenamed('id', 'laureate_gender_id')

    nobels_df = nobels_df.join(countries_df, countries_df["name"] == nobels_df['laureate_birth_country'], "left_outer") \
        .drop('laureate_birth_country', 'code', 'name', 'area', 'climate_id', 'population', 'population_density',
              'net_migration', 'infant_mortality', 'birthrate', 'deathrate', 'literacy', 'gdp', 'gdp_agriculture',
              'gdp_industry', 'gdp_service') \
        .withColumnRenamed('id', 'laureate_birth_country_id')

    nobels_df = nobels_df.join(cities_df, (cities_df["name"] == nobels_df['laureate_birth_city']) & (
            cities_df["country_id"] == nobels_df['laureate_birth_country_id']), "left_outer") \
        .drop('laureate_birth_city', 'laureate_birth_country_id', 'name', 'country_id', 'population', 'laureate_gender') \
        .withColumnRenamed('id', 'laureate_birth_city_id')

    nobels_df = nobels_df.withColumn('id', f.row_number().over(Window.orderBy("prize_year"))) \
        .select('id', 'prize_year', 'category_id', 'laureate_type_id', 'laureate_full_name', 'laureate_gender_id',
                'laureate_birth_date', 'laureate_birth_city_id')

    nobels_df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "gzip") \
        .saveAsTable(f"{DB_DWH}.nobel_prizes")

    spark.stop()


def create_dataset_table():
    spark = init_spark_session()

    laureates_df = get_laureates_df(spark)
    countries_df = get_countries_df(spark)
    cities_df = get_cities_df(spark, countries_df)
    climates_df = get_climates_df(spark)

    countries_birth_df = countries_df.select([f.col(c).alias("birth_country_" + c) for c in countries_df.columns])

    dataset_df = laureates_df.join(countries_birth_df,
                                   countries_birth_df["birth_country_name"] == laureates_df['laureate_birth_country'],
                                   "left_outer") \
        .drop('birth_country_name', 'birth_country_code')

    dataset_df = dataset_df.join(climates_df, climates_df["id"] == dataset_df['birth_country_climate_id'], "left_outer") \
        .drop('birth_country_climate_id', 'id') \
        .withColumnRenamed('description', 'birth_country_climate')

    dataset_df = dataset_df.join(cities_df, (cities_df["name"] == dataset_df['laureate_birth_city']) & (
                cities_df["country_id"] == dataset_df['birth_country_id']), "left_outer") \
        .drop('id', 'name', 'country_id', 'birth_country_id') \
        .withColumnRenamed('population', 'birth_city_population')

    dataset_df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "gzip") \
        .saveAsTable(f"{DB_DWH}.ds_laureates")

    spark.stop()


create_snowflake = PythonOperator(task_id='create_snowflake', python_callable=create_snowflake_tables, dag=dag)
create_dataset = PythonOperator(task_id='create_dataset', python_callable=create_dataset_table, dag=dag)

recreate_database >> [create_snowflake, create_dataset]
