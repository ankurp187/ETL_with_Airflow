
from airflow.decorators import dag, task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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


@dag(
    dag_id='bronze_file_processor_dag',
    params={
        "id": None,
        "filepath": None,
        "filename": None,
    },
    tags=['bronze']
)
def bronze_file_processor_dag():

    @task
    def file_processor(**context):
        ti = context['ti']
        try:
            filepath = context['params'].get('filepath')
            filename = context['params'].get('filename')
            table_name = filename[:-17]
            schema_name='bronze'
            df = pd.read_parquet(filepath)
            rec_cnt = len(df)
            print("Read",rec_cnt,"records in",filename,"for",table_name)
            data_tuples = [tuple(x) for x in df.to_numpy()]

            columns = df.columns.tolist()
            print(columns)

            insert_query = f"""
                INSERT INTO {schema_name}.{table_name} ({', '.join(columns)})
                VALUES %s
            """

            print(insert_query)

            conn = psycopg2.connect(
                host="postgres",           
                database="airflow",
                user="airflow",
                password="airflow",
                port=5432
            )
            
            with conn.cursor() as cur:
                # Use execute_values for fast bulk insert
                psycopg2.extras.execute_values(
                    cur, 
                    insert_query, 
                    data_tuples, 
                    page_size=5000
                )
                
            conn.commit()
            print(f"✅ Successfully inserted/updated {len(df)} rows using psycopg2")
            ti.xcom_push(key='status_value', value='success')
            ti.xcom_push(key='file_status_value', value='success')
            ti.xcom_push(key='inserted_recs', value=str(rec_cnt))
        except:
            ti.xcom_push(key='status_value', value='failed')
            ti.xcom_push(key='file_status_value', value='failed')
            ti.xcom_push(key='inserted_recs', value='-1')
    
    @task(task_display_name="Start Logging")
    def generate_writer_log(**context):
        id = context['params'].get('id')
        filename = context['params'].get('filename')
        table_name = filename[:-17]
        file_date = filename[-16:-8]

        return {
            "run_ID": id
            ,"run_start_time": datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')
            ,"run_end_time": 'NULL'
            ,"schema_name": 'bronze'
            ,"object_name": table_name
            ,"start_date": file_date
            ,"end_date": file_date
            ,"filename": filename
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
        filename = context['params'].get('filename')
        table_name = filename[:-17]
        file_date = filename[-16:-8]

        ti = context['ti']
        upstream_task_id = 'file_processor'

        task_status = ti.xcom_pull(task_ids=upstream_task_id, key='status_value')
        inserted_recs = ti.xcom_pull(task_ids=upstream_task_id, key='inserted_recs')

        return {
            "run_ID": id
            ,"run_end_time": datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')
            ,"schema_name": 'bronze'
            ,"object_name": table_name
            ,"start_date": file_date
            ,"end_date": file_date
            ,"filename": filename
            ,"inserted_records": inserted_recs
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


    @task
    def file_process_writer_log(**context):
        id = context['params'].get('id')
        filename = context['params'].get('filename')
        table_name = filename[:-17]
        file_date = filename[-16:-8]

        return {
        "type": 'insert',
        "id": id,
        "start_time": datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S'),
        "filename": filename,
        "status": None
        }
    
    log_file_writer_conf = file_process_writer_log()

    trigger_log_file_writer = TriggerDagRunOperator(
        task_id="trigger_file_log_writer",
        trigger_dag_id="file_processing_log_dag",
        conf=log_file_writer_conf,
        wait_for_completion=True
    )

    @task
    def file_process_updater_log(**context):
        id = context['params'].get('id')
        filename = context['params'].get('filename')
        table_name = filename[:-17]
        file_date = filename[-16:-8]

        ti = context['ti']
        upstream_task_id = 'file_processor'
        task_status = ti.xcom_pull(task_ids=upstream_task_id, key='file_status_value')

        return {
        "type": 'update',
        "id": id,
        "start_time": datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S'),
        "filename": filename,
        "status": task_status
        }
    
    log_file_updater_conf = file_process_updater_log()

    trigger_log_file_updater = TriggerDagRunOperator(
        task_id="trigger_file_log_updater",
        trigger_dag_id="file_processing_log_dag",
        conf=log_file_updater_conf,
        wait_for_completion=True
    )

    log_file_writer_conf >> trigger_log_file_writer >> log_writer_conf >> trigger_log_writer >> file_processor() >> log_updater_conf >> trigger_log_updater >> log_file_updater_conf >> trigger_log_file_updater


bronze_file_processor_dag()
