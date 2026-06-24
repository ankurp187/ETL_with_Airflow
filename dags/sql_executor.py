from airflow.decorators import dag, task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import psycopg2

@dag(
    dag_id='dynamic_postgres_dag',
    params={
        "sql_query": """""",
        "run_date": None
    },
    tags=['postgres', 'ddl', 'dynamic']
)
def dynamic_postgres_dag():

    @task
    def print_params(**context):
        """Just for visibility"""
        sql = context['params'].get('sql_query')
        print("SQL that will be executed:")
        print(sql)
        return sql

    # Execute dynamic SQL passed at runtime
    # execute_sql = PostgresOperator(
    #     task_id='execute_dynamic_sql',
    #     postgres_conn_id='postgres_default',      # ← Change if needed
    #     sql="{{ params.sql_query }}",             # ← This is the key: Jinja templating
    # )

    @task
    def execute_dynamic_sql(**context):
        sql = context['params'].get('sql_query')
        
        if not sql or not sql.strip().upper().startswith(('ALTER', 'CREATE', 'DROP', 'INSERT', 'UPDATE')):
            raise ValueError("Invalid or unsafe SQL passed!")
        
        # hook = PostgresHook(postgres_conn_id='postgres_default')
        # hook.run(sql)
        # print("✅ SQL executed successfully")

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

    print_params() >> execute_dynamic_sql()


# Instantiate the DAG
dynamic_postgres_dag()