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

def generate_daily_source(run_date):
    NUM_CUSTOMERS = 800
    NUM_ACCOUNTS = 1200
    BASE_SEED = 42

    # print("Printing inside function",run_date)

    OUTPUT_DIR = f"/opt/airflow/data/tm_source_data/{run_date}"

    os.makedirs(os.path.dirname(OUTPUT_DIR+'/'),exist_ok=True)

    print("Printing output directory 2 ",OUTPUT_DIR)

    np.random.seed(BASE_SEED + int(run_date) % 10000)  # Reproducible per day

    print(f"Generating source data for: {run_date}")

    # ------------------ Customers (Master + Daily Delta) ------------------
    customers = pd.DataFrame({
        'customer_id': [f'CUST{str(i).zfill(6)}' for i in range(1, NUM_CUSTOMERS + 1)],
        'full_name': [f'Customer {i}' for i in range(1, NUM_CUSTOMERS + 1)],
        'date_of_birth': pd.date_range('1960-01-01', periods=NUM_CUSTOMERS).date,
        'nationality': np.random.choice(['US', 'UK', 'IN', 'BR', 'NG', 'CN'], NUM_CUSTOMERS),
        'occupation': np.random.choice(['Engineer', 'Doctor', 'Business Owner', 'Student', 'Retired'], NUM_CUSTOMERS),
        'risk_score': np.random.randint(10, 95, NUM_CUSTOMERS),
        'record_date': run_date                     # For SCD2 tracking
    })

    # ------------------ Accounts (Master + Daily Delta) ------------------
    account_types = ['Savings', 'Checking', 'Credit Card', 'Investment', 'Loan']
    accounts = pd.DataFrame({
        'account_id': [f'ACC{str(i).zfill(7)}' for i in range(1, NUM_ACCOUNTS + 1)],
        'account_type': np.random.choice(account_types, NUM_ACCOUNTS),
        'currency': np.random.choice(['USD', 'EUR', 'GBP'], NUM_ACCOUNTS),
        'open_date': pd.date_range(start='2023-01-01', periods=NUM_ACCOUNTS).date,
        'status': np.random.choice(['Active', 'Dormant', 'Closed'], NUM_ACCOUNTS, p=[0.88, 0.08, 0.04]),
        'balance': np.random.lognormal(8.5, 2.2, NUM_ACCOUNTS).round(2),
        'record_date': run_date
    })

    # ------------------ Account-Customer Relationship ------------------
    ac_rel = []
    for acc in accounts['account_id']:
        num_owners = np.random.choice([1, 2, 3], p=[0.78, 0.18, 0.04])
        owners = np.random.choice(customers['customer_id'], num_owners, replace=False)
        for owner in owners:
            ac_rel.append({
                'account_id': acc,
                'customer_id': owner,
                'relationship_type': np.random.choice(['Primary Owner', 'Joint Owner', 'Authorized', 'Beneficiary']),
                'start_date': run_date
            })
    account_customer = pd.DataFrame(ac_rel)

    # ------------------ Customer-Customer Relationship ------------------
    cc_rel = []
    for _ in range(int(NUM_CUSTOMERS * 0.12)):   # Reasonable number of relationships
        c1, c2 = np.random.choice(customers['customer_id'], 2, replace=False)
        cc_rel.append({
            'customer_id1': c1,
            'customer_id2': c2,
            'relationship_type': np.random.choice(['Family', 'Business Partner', 'Close Associate', 'Employer']),
            'start_date': run_date
        })
    customer_customer = pd.DataFrame(cc_rel)

    # ------------------ Transactions (Only for this day) ------------------
    num_tx = np.random.randint(8000, 15000)   # Realistic daily volume

    transactions = []

    for i in range(num_tx):
        acc_from = np.random.choice(accounts['account_id'])
        acc_to = np.random.choice(accounts['account_id'])
        while acc_to == acc_from:
            acc_to = np.random.choice(accounts['account_id'])

        amount = round(np.random.lognormal(6.5, 1.8), 2)
        if np.random.rand() < 0.06:          # High value transactions
            amount *= np.random.uniform(5, 12)
        
        time = datetime.now().strftime('%H:%M:S%')
        run_tmstmp = str(run_date)[:4]+'-'+str(run_date)[4:6]+'-'+str(run_date)[6:8] + ' ' + time

        transactions.append({
            'transaction_id': f'TX{run_date}{str(i+1).zfill(6)}',
            'transaction_date': run_date,
            'transaction_timestamp': run_tmstmp,
            'account_from': acc_from,
            'account_to': acc_to,
            'amount': amount,
            'currency': 'USD',
            'transaction_type': np.random.choice(['Transfer', 'Wire', 'Payment', 'Deposit', 'Withdrawal']),
            'description': np.random.choice(['Salary Credit', 'Rent Payment', 'Online Shopping', 'Vendor Payment', 'ATM Withdrawal', 'International Transfer']),
            'channel': np.random.choice(['Mobile App', 'Internet Banking', 'Branch', 'ATM', 'Third Party'])
        })

    transactions = pd.DataFrame(transactions)

    print("Generating Transactions MasterData")

    # ====================== SAVE SOURCE FILES ======================
    date_str = run_date

    customers.to_parquet(f"{OUTPUT_DIR}/customers_{date_str}.parquet", index=False)
    accounts.to_parquet(f"{OUTPUT_DIR}/accounts_{date_str}.parquet", index=False)
    account_customer.to_parquet(f"{OUTPUT_DIR}/account_customer_{date_str}.parquet", index=False)
    customer_customer.to_parquet(f"{OUTPUT_DIR}/customer_customer_{date_str}.parquet", index=False)
    transactions.to_parquet(f"{OUTPUT_DIR}/transactions_{date_str}.parquet", index=False)

    print(f"✓ Source data generated for {run_date}")
    print(f"   Customers: {len(customers)}, Accounts: {len(accounts)}, Transactions: {len(transactions)}")

    return date_str


@dag(
    dag_id='source_data_dag',
    schedule='0 6 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['source_data'],
    doc_md="""
    # Daily Source Data generator for the project

    This dag gets triggered on daily basis at 6 UTC. 
    The task **generate_source_data** fetches run_date from metadata and calls **generate_daily_source** with run_date.
    The function **generate_daily_source** takes input from Metadata and creates source data in parquet format.
    """
)
def source_data_dag():
    
    @task(
        doc_md="""
        ## Generate source data

        **psycopg2:** This module is used to create connection with postgres
        The run_date is fetched with the help of this connection and calls for **generate_daily_source**
        """,
        task_display_name = "Generate Source Data"
    )
    def generate_source_data(**context):

        conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
        )

        sql = "select run_date from staging.metadata where layer_name='raw_data'"

        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        run_date = cursor.fetchone()[0]
        conn.commit()
        print(run_date)

        print("Before processing for",run_date)
        ti = context['ti'] 
        try:
            date_str = generate_daily_source(run_date)
            print("Files processed for",date_str)
            date1 = datetime.strptime(run_date, "%Y%m%d")
            print(date1)
            next_date = date1 + timedelta(days=1)
            print(next_date)
            next_date_1 = next_date.strftime('%Y%m%d')
            print(next_date_1)

            sql = f"update staging.metadata set run_date = {next_date_1} where layer_name='raw_data'"

            cursor = conn.cursor()
            print(sql)
            cursor.execute(sql)
            conn.commit()

            ti.xcom_push(key='status_value', value='success')
        except:
            ti.xcom_push(key='status_value', value='failed')
        print("After processing",run_date)
    
    @task(
            task_display_name="Start Logging"
    )
    def generate_writer_log(**context):
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        try_number = context['task_instance'].try_number

        print(dag_id)
        print(run_id[:-13])
        print(try_number)

        run_id = run_id[:-13]
        seconds = run_id[-2:]
        minutes = run_id[-16:][-2:]
        run_id = run_id[:-6]+'_'+minutes+'_'+seconds

        id = dag_id+'_'+run_id
        print(id)

        return {
            "run_ID": id
            ,"run_start_time": datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')
            ,"run_end_time": 'NULL'
            ,"schema_name": 'raw_data'
            ,"object_name": 'all_files'
            ,"start_date": 'NULL'
            ,"end_date": 'NULL'
            ,"filename": 'NULL'
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
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        try_number = context['task_instance'].try_number

        print(try_number)

        run_id = run_id[:-13]
        seconds = run_id[-2:]
        minutes = run_id[-16:][-2:]
        run_id = run_id[:-6]+'_'+minutes+'_'+seconds

        ti = context['ti']
        upstream_task_id = 'generate_source_data'

        id = dag_id+'_'+run_id
        print(id)

        task_status = ti.xcom_pull(task_ids=upstream_task_id, key='status_value')

        return {
            "run_ID": id
            ,"run_end_time": datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')
            ,"schema_name": 'raw_data'
            ,"object_name": 'all_files'
            ,"start_date": 'NULL'
            ,"end_date": 'NULL'
            ,"filename": 'NULL'
            ,"inserted_records": 'NULL'
            ,"updated_records": 'NULL'
            ,"comments": task_status
        }
    
    log_updater_conf = generate_updater_log()

    trigger_log_updater = TriggerDagRunOperator(
        task_id="trigger_log_updater",
        trigger_dag_id="log_enddate_updater_dag",
        conf=log_updater_conf,
        wait_for_completion=True
    )
    
    log_writer_conf >> trigger_log_writer >> generate_source_data() >> log_updater_conf >> trigger_log_updater

source_data_dag()
