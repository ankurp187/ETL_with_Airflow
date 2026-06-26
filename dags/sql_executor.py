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
        """Just for visibility of context parameters"""
        sql = context['params'].get('sql_query')
        print("SQL that will be executed:")
        print(sql)
        return sql

    @task(
            doc_md="""
            ## SQL Query Executor

            **psycopg2:** This module is used to create connection with postgres
            The query is executed with the help of this connection.
            """,
            task_display_name = "SQL Executor"
    )
    def execute_dynamic_sql(**context):
        sql = context['params'].get('sql_query')
        
        if not sql or not sql.strip().upper().startswith(('ALTER', 'CREATE', 'DROP', 'INSERT', 'UPDATE')):
            raise ValueError("Invalid or unsafe SQL passed!")

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

        print("✅ SQL executed successfully")

        conn.close()

    print_params() >> execute_dynamic_sql()


dynamic_postgres_dag()
