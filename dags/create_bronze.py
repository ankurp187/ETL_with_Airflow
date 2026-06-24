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
    params={
        "run_date": None
    },
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

        run_date = context['params'].get('run_date')
        OUTPUT_DIR = f"/opt/airflow/data/tm_source_data/{run_date}"
        for filename in os.listdir(OUTPUT_DIR+'/'):
            filepath = os.path.join(OUTPUT_DIR,filename)
            is_file_executed = file_tester(filepath,filename)
            if not is_file_executed:
                run_param = {"id": id,"filepath": filepath,"filename": filename}
                trigger_params.append(run_param)
        
        print(f"Will trigger child DAG {len(trigger_params)} times")
        return trigger_params

    # @task
    # def file_ingestor(trigger_list: list, **context):
    #     """Trigger child DAG multiple times"""
    #     triggered_runs = []
        
    #     for i, params in enumerate(trigger_list):
    #         trigger = TriggerDagRunOperator(
    #             task_id=f'trigger_child_{params["filename"]}',   # Unique task_id
    #             trigger_dag_id='bronze_file_processor_dag',      # Child DAG ID
    #             conf=params,                                   # Pass parameters
    #             wait_for_completion=False,                     # Set True if you want sequential
    #             reset_dag_run=True,
    #             dag=context['dag'],                            # Important
    #         )
    #         trigger.execute(context=context)                   # Execute immediately
    #         triggered_runs.append(params["filename"])
    #         print(f"Triggered child DAG for region: {params['filename']}")
        
    #     return triggered_runs

    trigger_tasks = TriggerDagRunOperator.partial(
        task_id='trigger_child_dag',
        trigger_dag_id='bronze_file_processor_dag',
        wait_for_completion=False,
        reset_dag_run=True,
    ).expand(
        conf=get_trigger_list()          # This creates multiple mapped tasks
    )

    # Task Flow
    # params_list = get_trigger_list()
    # file_ingestor(params_list)

    get_trigger_list() >> trigger_tasks


bronze_dag()
