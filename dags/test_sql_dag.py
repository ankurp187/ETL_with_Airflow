from airflow.decorators import dag, task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

@dag(
    dag_id='test_sql_dag'
)
def test_sql_dag():

    @task
    def test_sql(**context):
        conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
        )

        sql = "select run_date from staging.metadata where layer_name='bronze'"

        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        run_date = cursor.fetchone()[0]
        conn.commit()
        run_date = run_date[:4]+run_date[5:7]+run_date[8:10]
        print(run_date)

        date1 = datetime.strptime(run_date, "%Y%m%d")
        print(date1)
        next_date = date1 + timedelta(days=1)
        print(next_date)
        next_date_1 = next_date.strftime('%Y-%m-%d')
        print(next_date_1)

        sql = f"update staging.metadata set run_date = '{next_date_1}' where layer_name='bronze'"

        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        conn.commit()

    test_sql()

test_sql_dag()