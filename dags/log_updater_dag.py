from airflow.decorators import dag, task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import psycopg2

@dag(
    dag_id='log_enddate_updater_dag',
    params={
        "run_ID": None
        ,"run_end_time": None
        ,"schema_name": None
        ,"object_name": None
        ,"start_date": None
        ,"end_date": None
        ,"filename": None
        ,"inserted_records": None
        ,"updated_records": None
        ,"comments": None
    },
    tags=['postgres', 'ddl', 'dynamic']
)
def log_enddate_updater_dag():

    @task
    def execute_dynamic_sql(**context):

        run_ID = context['params'].get('run_ID')
        run_end_time = context['params'].get('run_end_time')
        schema_name = context['params'].get('schema_name')
        object_name = context['params'].get('object_name')
        start_date = context['params'].get('start_date')
        end_date = context['params'].get('end_date')
        filename = context['params'].get('filename')
        inserted_records = context['params'].get('inserted_records')
        updated_records = context['params'].get('updated_records')
        comments = context['params'].get('comments')

        sql = f"update log.logging_table set run_end_time='{run_end_time}', inserted_records='{inserted_records}', updated_records='{updated_records}', comments='{comments}' where run_ID='{run_ID}' and schema_name='{schema_name}' and object_name='{object_name}' and start_date='{start_date}' and end_date='{end_date}' and filename='{filename}';"

        print(sql)

        conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
        )

        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        conn.commit()

        # count = cursor.fetchone()[0]
        # print(count)
        print("✅ SQL executed successfully")

        conn.close()

    execute_dynamic_sql()


# Instantiate the DAG
log_enddate_updater_dag()