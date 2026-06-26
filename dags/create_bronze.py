from airflow.decorators import dag, task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from io import BytesIO
from processing_info import file_processing_log_dag

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

from bronze_file_processor_dag import bronze_file_processor_dag

@dag(
    dag_id='bronze_dag',
    tags=['postgres', 'ddl', 'dynamic']
)
def bronze_dag():

    def file_tester(filepath,filename):
        conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
        )

        sql = f"select count(*) cnt from log.file_process_status where filename='{filename}' and run_status='success'"
        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        conn.commit()
        cnt = cursor.fetchone()[0]
        print("Testing:",filepath,"Got",cnt,"records")
        return cnt
    
    @task
    def get_trigger_list(**context):

        trigger_params = []

        dag_id = context['dag'].dag_id
        run_id = context['run_id']

        print(dag_id)
        print(run_id[:-13])

        run_id = run_id[:-13]
        seconds = run_id[-2:]
        minutes = run_id[-16:][-2:]
        run_id = run_id[:-6]+'_'+minutes+'_'+seconds

        id = dag_id+'_'+run_id
        print(id)

        # run_date = context['params'].get('run_date')
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

        OUTPUT_DIR = f"/opt/airflow/data/tm_source_data/{run_date}"
        for filename in os.listdir(OUTPUT_DIR+'/'):
            filepath = os.path.join(OUTPUT_DIR,filename)
            is_file_executed = file_tester(filepath,filename)
            if not is_file_executed:
                run_param = {"id": id,"filepath": filepath,"filename": filename}
                trigger_params.append(run_param)
        
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
        
        print(f"Will trigger child DAG {len(trigger_params)} times")
        return trigger_params


    trigger_tasks = TriggerDagRunOperator.partial(
        task_id='trigger_child_dag',
        trigger_dag_id='bronze_file_processor_dag',
        wait_for_completion=False,
        reset_dag_run=True,
    ).expand(
        conf=get_trigger_list()          # This creates multiple mapped tasks
    )


    get_trigger_list() >> trigger_tasks


bronze_dag()
