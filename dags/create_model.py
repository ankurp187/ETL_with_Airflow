
from airflow.decorators import dag, task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import psycopg2
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from processing_info import file_processing_log_dag
from log_writer_dag import log_writer_dag
from log_updater_dag import log_enddate_updater_dag

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

def insert_data_in_staging(run_date, table_name, pk_cols, non_pk_cols, staging_table, target_table):
    # We can call a stored procedure and use its output to populate staging table. For now, we will do a direct copy from bronze.
    collist = ','.join(pk_cols+non_pk_cols)
    
    conn = psycopg2.connect(
    host="postgres",
    database="airflow",
    user="airflow",
    password="airflow",
    port=5432
    )

    cursor = conn.cursor()

    sql1 = f'truncate table staging.{table_name}' 
    print(sql1)
    cursor.execute(sql1)
    conn.commit()
    
    sql2 = f'INSERT INTO staging.{table_name} ({collist}) SELECT {collist} from bronze.{table_name}' 
    print(sql2)
    cursor.execute(sql2)
    conn.commit()

    print("✅ SQL executed successfully")


    with conn.cursor() as cur:
        cur.execute("""
            CALL staging.scd2_upsert(
                p_run_date       := %s,
                p_staging_table  := %s,
                p_target_table   := %s,
                p_pk_columns     := %s,
                p_non_pk_columns := %s
            );
        """, (
            run_date,
            staging_table,
            target_table,
            pk_cols,
            non_pk_cols
        ))
    conn.commit()
    print("Stored Procedure executed successfully")

    conn.close()


@dag(
    dag_id='create_model_dag',
    params={
        "id":None,
        "date": None,
        "staging_table": None,
        "target_table": None,
        "pk_columns": None,
        "non_pk_columns": None
    },
    tags=['silver'],
    doc_md="""
    # Create Model tables

    This dag is used to create model tables and log the status. 
    """
)
def create_model_dag():

    @task(
            task_display_name="Ingest into Model Table"
    )
    def model_processor(**context):
        ti = context['ti']
        try:
            id = context['params'].get('id')
            date = context['params'].get('date')
            staging_table = context['params'].get('staging_table')
            target_table = context['params'].get('target_table')
            pk_columns = context['params'].get('pk_columns')
            non_pk_columns = context['params'].get('non_pk_columns')
            insert_data_in_staging(date, target_table.split('.')[1], pk_columns, non_pk_columns, staging_table, target_table)
            ti.xcom_push(key='status_value', value='success')
        except:
            ti.xcom_push(key='status_value', value='failed')

    
    @task(task_display_name="Start Logging")
    def generate_writer_log(**context):
        id = context['params'].get('id')
        date = context['params'].get('date')
        target_table = context['params'].get('target_table')

        return {
            "run_ID": id
            ,"run_start_time": datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')
            ,"run_end_time": 'NULL'
            ,"schema_name": 'silver'
            ,"object_name": target_table.split('.')[1]
            ,"start_date": date
            ,"end_date": date
            ,"filename": ''
            ,"inserted_records": 'NULL'
            ,"updated_records": 'NULL'
            ,"comments": 'NULL'
        }
    
    log_writer_conf = generate_writer_log()

    trigger_log_writer = TriggerDagRunOperator(
        task_id="trigger_log_writer",
        trigger_dag_id="log_writer_dag",
        conf=log_writer_conf,
        wait_for_completion=True
    )


    @task(trigger_rule='all_done',task_display_name="Update Load Status")
    def generate_updater_log(**context):
        id = context['params'].get('id')
        date = context['params'].get('date')
        target_table = context['params'].get('target_table')

        ti = context['ti']
        upstream_task_id = 'model_processor'

        task_status = ti.xcom_pull(task_ids=upstream_task_id, key='status_value')

        return {
            "run_ID": id
            ,"run_end_time": datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')
            ,"schema_name": 'silver'
            ,"object_name": target_table
            ,"start_date": date
            ,"end_date": date
            ,"filename": ''
            ,"inserted_records": 0
            ,"updated_records": 0
            ,"comments": task_status
        }
    
    log_updater_conf = generate_updater_log()

    trigger_log_updater = TriggerDagRunOperator(
        task_id="trigger_log_updater",
        trigger_dag_id="log_enddate_updater_dag",
        conf=log_updater_conf,
        wait_for_completion=True
    )

    log_writer_conf >> trigger_log_writer >> model_processor() >> log_updater_conf >> trigger_log_updater


create_model_dag()
