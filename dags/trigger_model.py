
from airflow.decorators import dag, task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


from log_writer_dag import log_writer_dag
from log_updater_dag import log_enddate_updater_dag

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys



@dag(
    dag_id='trigger_model_dag',
    params={
        "run_date": None
    },
    tags=['postgres', 'ddl', 'dynamic']
)
def trigger_model_dag():


    @task
    def get_table_configs(**context):
        run_date = context['params'].get('run_date')

        dag_id = context['dag'].dag_id
        run_id = context['run_id']

        run_id = run_id[:-13]
        seconds = run_id[-2:]
        minutes = run_id[-16:][-2:]
        run_id = run_id[:-6]+'_'+minutes+'_'+seconds

        id = dag_id+'_'+run_id
        print(id)
        
        return [
            {
                "id":id,
                "date": run_date,
                "staging_table": "staging.customers",
                "target_table": "silver.customers",
                "pk_columns": ["customer_id"],
                "non_pk_columns": ['full_name', 'date_of_birth', 'nationality', 'occupation', 'risk_score', 'record_date']
            },
            {
                "id":id,
                "date": run_date,
                "staging_table": "staging.accounts",
                "target_table": "silver.accounts",
                "pk_columns": ['account_id'],
                "non_pk_columns": ['account_type', 'currency', 'open_date', 'status', 'balance', 'record_date']
            },
            {
                "id":id,
                "date": run_date,
                "staging_table": "staging.account_customer",
                "target_table": "silver.account_customer",
                "pk_columns": ['account_id', 'customer_id'],
                "non_pk_columns": ['relationship_type', 'start_date']
            },
            {
                "id":id,
                "date": run_date,
                "staging_table": "staging.customer_customer",
                "target_table": "silver.customer_customer",
                "pk_columns": ['customer_id1', 'customer_id2'],
                "non_pk_columns": ['relationship_type', 'start_date']
            },
            {
                "id":id,
                "date": run_date,
                "staging_table": "staging.transactions",
                "target_table": "silver.transactions",
                "pk_columns": ["transaction_id"],
                "non_pk_columns": ['transaction_date', 'transaction_timestamp', 'account_from', 'account_to', 'amount', 'currency', 'transaction_type', 'description', 'channel']
            }
        ]

    # This will create one task per table dynamically
    trigger_tasks = TriggerDagRunOperator.partial(
        task_id='trigger_child_dag',
        trigger_dag_id='create_model_dag',
        wait_for_completion=False,
        reset_dag_run=True,
    ).expand(
        conf=get_table_configs()          # This creates multiple mapped tasks
    )
    
    get_table_configs() >> trigger_tasks

trigger_model_dag()

