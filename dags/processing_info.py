from airflow.decorators import dag, task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import psycopg2

@dag(
    dag_id='file_processing_log_dag',
    params={
        "type": None,
        "id": None,
        "start_time": None,
        "filename": None,
        "status": None
    },
    tags=['logging']
)
def file_processing_log_dag():

    @task(task_display_name="File Processing Logs")
    def execute_dynamic_sql(**context):
        type = context['params'].get('type')
        id = context['params'].get('id')
        filename = context['params'].get('filename')

        conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
        )

        sql = None

        print("Call Received:",type,id,filename)

        if type.lower()=='insert':
            start_time = context['params'].get('start_time')
            print("Call Received 1:",type,id,filename)
            sql = f"Insert into log.file_process_status VALUES ('{id}','{start_time}','In-Progress','{filename}')"
            print("Call Received 2:",type,id,filename)
        elif type.lower()=='update':
            status = context['params'].get('status')
            print("Call Received 3:",type,id,filename)
            sql = f"Update log.file_process_status set run_status='{status}' where run_id='{id}' and filename='{filename}'"
            print("Call Received 4:",type,id,filename)

        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        conn.commit()

        print("✅ SQL executed successfully")

        conn.close()

    execute_dynamic_sql()


file_processing_log_dag()
