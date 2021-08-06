import csv
import datetime
import json
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from textwrap import dedent

# настройки
DAG_NAME = 'ai_staging'
DB_NAME = 'undkit_air_staging'

# наш основной DAG
dag = DAG(dag_id=DAG_NAME, schedule_interval=None, start_date=datetime.datetime(2021, 8, 4), catchup=False)

# bash commands
exec_hive_command = dedent("""
/usr/bin/beeline -u jdbc:hive2://10.93.1.9:10000 -n hive -p cloudera -e "{{ params.sql }}"
""")

sqoop_import_command = dedent("""
hdfs dfs -rm -r -f -skipTrash {{ params.table }} >/dev/null 2>&1

export JAVA_HOME="/usr"
/usr/lib/sqoop/bin/sqoop import --connect "jdbc:mysql://10.93.1.9/skillfactory" \
        --username mysql --password arenadata --hive-import -m 1 \
        --table {{ params.table }} --hive-table {{ params.db }}.{{ params.table }}
""")

copy_to_hdfs_command = dedent("""
hdfs dfs -mkdir -p {{ params.hdfs_dir }}
hdfs dfs -put -f {{ params.source_file }} {{ params.hdfs_dir }}/{{ params.hdfs_file }}
""")

delete_from_hdfs_command = dedent("""
hdfs dfs -rm -r -f -skipTrash {{ params.hdfs_dir }} >/dev/null 2>&1
""")

# OPERATORS

# init db
recreate_database = BashOperator(
    dag=dag,
    task_id='recreate_staging_db',
    bash_command=exec_hive_command,
    params={"sql": """
        DROP DATABASE IF EXISTS {db} CASCADE;
        CREATE DATABASE {db};
    """.format(db=DB_NAME)},
)


def create_table_from_csv(parent_dag: DAG, table_name, file_name, create_fields, select_fields):
    child_dag = DAG(f"{parent_dag.dag_id}.create_from_csv_{table_name}",
                    schedule_interval=parent_dag.schedule_interval,
                    start_date=parent_dag.start_date,
                    catchup=parent_dag.catchup)

    # скопировать файл в *HDFS*
    copy_to_hdfs = BashOperator(
        dag=child_dag,
        task_id=f'copy_to_hdfs_{table_name}',
        bash_command=copy_to_hdfs_command,
        params={"hdfs_dir": f"/tmp/ai/{table_name}", "hdfs_file": "data.csv", "source_file": file_name},
    )

    # создать внешнюю таблицу
    create_table = BashOperator(
        dag=child_dag,
        task_id=f'create_table_{DB_NAME}.{table_name}_orc',
        bash_command=exec_hive_command,
        params={"sql": f"""
            /* удалить внешнюю таблицу */
            DROP TABLE IF EXISTS {DB_NAME}.{table_name};
        
            /*создать внешнюю таблицу*/
            CREATE EXTERNAL TABLE {DB_NAME}.{table_name}
            (
                {create_fields}
            )
            COMMENT '{file_name}'
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            LOCATION '/tmp/ai/{table_name}'
            TBLPROPERTIES ('skip.header.line.count' = '1');
            
            /*удалить предыдущий вариант таблицы*/
            DROP TABLE IF EXISTS {DB_NAME}.{table_name}_orc;
            
            /*создать таблицу нужного формата*/
            CREATE TABLE {DB_NAME}.{table_name}_orc STORED AS ORC AS 
            SELECT {select_fields}
            FROM {DB_NAME}.{table_name};    
            
            /*удалить внешнюю таблицу*/
            DROP TABLE {DB_NAME}.{table_name} PURGE;    
        """},
    )

    # удалить все файлы из директории с таблицей из *HDFS*.
    delete_from_hdfs = BashOperator(
        dag=child_dag,
        task_id=f'delete_from_hdfs_{table_name}',
        bash_command=delete_from_hdfs_command,
        params={"hdfs_dir": f"/tmp/ai/{table_name}"},
    )

    copy_to_hdfs >> create_table >> delete_from_hdfs

    return child_dag


# staging.cities
import_cities = BashOperator(
    dag=dag,
    task_id='import_cities',
    bash_command=sqoop_import_command,
    params={"db": DB_NAME, "table": "cities"},
)

store_cities = BashOperator(
    dag=dag,
    task_id='store_cities',
    bash_command=exec_hive_command,
    params={"sql": """
        DROP TABLE IF EXISTS {orc_table} PURGE;
        CREATE TABLE {orc_table} STORED AS ORC AS SELECT * FROM {hive_table};
        DROP TABLE {hive_table} PURGE;
    """.format(hive_table=f"{DB_NAME}.cities", orc_table=f"{DB_NAME}.cities_orc")},
)


# json
def json2csv(in_path, out_path, headers):
    with open(in_path, "r") as json_file, open(out_path, "w") as csv_file:
        dictionary = json.load(json_file)

        file_writer = csv.writer(csv_file, delimiter=",", lineterminator="\n")
        file_writer.writerow(headers)

        for key, value in dictionary.items():
            file_writer.writerow([key, value])


convert_tasks = []

for in_path, out_path, headers in [
    ('/home/deng/Data/phone.json', '/tmp/ai/phone.csv', ['country_code', 'phone_code']),
    ('/home/deng/Data/names.json', '/tmp/ai/names.csv', ['country_code', 'country_name']),
    ('/home/deng/Data/iso3.json', '/tmp/ai/iso3.csv', ['iso2_country_code', 'iso3_country_code']),
    ('/home/deng/Data/currency.json', '/tmp/ai/currency.csv', ['country_code', 'currency_code']),
    ('/home/deng/Data/continent.json', '/tmp/ai/continent.csv', ['country_code', 'continent_code']),
    ('/home/deng/Data/capital.json', '/tmp/ai/capital.csv', ['country_code', 'capital'])
]:
    convert_task = PythonOperator(
        task_id='convert_' + in_path.split('/')[-1],
        python_callable=json2csv,
        op_kwargs={'in_path': in_path, 'out_path': out_path, 'headers': headers},
        dag=dag,
    )

    convert_tasks.append(convert_task)

# csv
country_create_fields = """
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
"""

country_select_fields = """
TRIM(country)                                              AS country,
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
CAST(REPLACE(service, ',', '.') AS DECIMAL(4, 3))          AS service
"""

nobel_create_fields = """
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
"""

nobel_select_fields = """
CAST(year AS smallint),
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
death_country
"""

csv_tasks = []

for file_name, table_name, create_fields, select_fields in [
    ("/tmp/ai/phone.csv", "phones", "country_code STRING, phone_code STRING", "*"),
    ("/tmp/ai/names.csv", "country_names", "country_code STRING, country_name STRING", "*"),
    ("/tmp/ai/iso3.csv", "iso3", "iso2_country_code STRING, iso3_country_code STRING", "*"),
    ("/tmp/ai/currency.csv", "currencies", "country_code  STRING, currency_code STRING", "*"),
    ("/tmp/ai/continent.csv", "continent", "country_code  STRING, continent_code STRING", "*"),
    ("/tmp/ai/capital.csv", "capitals", "country_code  STRING, capital STRING", "*"),
    ("/home/deng/Data/countries_of_the_world.csv", "countries", country_create_fields, country_select_fields),
    ("/home/deng/Data/nobel-laureates.csv", "nobel_laureates", nobel_create_fields, nobel_select_fields),
]:
    csv_task = SubDagOperator(subdag=create_table_from_csv(dag, table_name, file_name, create_fields, select_fields),
                              task_id=f"create_from_csv_{table_name}",
                              dag=dag,
                              trigger_rule='all_done')
    csv_tasks.append(csv_task)

prepare_json = DummyOperator(task_id='prepare_json', dag=dag)
prepare_csv = DummyOperator(task_id='prepare_csv', dag=dag)

recreate_database >> import_cities >> store_cities
recreate_database >> prepare_json >> convert_tasks >> prepare_csv >> csv_tasks



#convert_tasks << prepare_csv
#prepare_csv.set_upstream(convert_tasks)

#prepare_csv >> csv_tasks

