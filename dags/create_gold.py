
from airflow.decorators import dag, task
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import psycopg2
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id='create_gold_dag'
)
def create_gold_dag():

    @task
    def daily_account_summary():
        conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
        )

        sql = """TRUNCATE TABLE gold.daily_account_summary;
                INSERT INTO gold.daily_account_summary
                SELECT 
                    a.account_id,
                    t.transaction_date,
                    a.account_type,
                    COUNT(*) AS total_transactions,
                    SUM(cast(t.amount as decimal)) AS total_amount,
                    AVG(cast(t.amount as decimal)) AS avg_amount,
                    MAX(cast(t.amount as decimal)) AS max_amount,
                    COUNT(CASE WHEN cast(t.amount as decimal) > 50000 THEN 1 END) AS high_value_count,
                    COUNT(DISTINCT t.account_to) AS distinct_counterparties,
                    NOW() AS loaded_at
                FROM silver.transactions t
                JOIN silver.accounts a ON t.account_from = a.account_id
                WHERE t.transaction_date = CURRENT_DATE - 1        
                GROUP BY a.account_id, t.transaction_date, a.account_type;"""

        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        conn.commit()
    
    @task
    def customer_risk_profile():
        conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
        )

        sql = """TRUNCATE TABLE gold.customer_risk_profile;
                INSERT INTO gold.customer_risk_profile
                SELECT 
                    c.customer_id,
                    c.full_name,
                    c.risk_score,
                    COUNT(t.transaction_id) AS total_txn_count,
                    SUM(t.amount) AS total_volume,
                    AVG(t.amount) AS avg_txn_amount,
                    COUNT(CASE WHEN t.is_suspicious_flag = 1 THEN 1 END) AS suspicious_txn_count,
                    ROUND(COUNT(CASE WHEN t.is_suspicious_flag = 1 THEN 1 END)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS high_risk_txn_ratio,
                    COUNT(DISTINCT t.account_to) AS distinct_counterparties,
                    MAX(t.amount) AS max_single_txn,
                    MAX(t.transaction_date) AS last_transaction_date,
                    CASE 
                        WHEN c.risk_score >= 80 OR COUNT(CASE WHEN t.is_suspicious_flag = 1 THEN 1 END) >= 5 THEN 'Very High'
                        WHEN c.risk_score >= 60 OR COUNT(CASE WHEN t.is_suspicious_flag = 1 THEN 1 END) >= 3 THEN 'High'
                        WHEN c.risk_score >= 40 THEN 'Medium'
                        ELSE 'Low'
                    END AS risk_category,
                    NOW() AS loaded_at
                FROM silver.customers c
                LEFT JOIN silver.transactions t ON c.customer_id = t.customer_from
                WHERE t.transaction_date >= CURRENT_DATE - 30 
                GROUP BY c.customer_id, c.full_name, c.risk_score;"""

        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        conn.commit()
    
    @task
    def monthly_customer_metrics():
        conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
        )

        sql = """TRUNCATE TABLE gold.monthly_customer_metrics;
                INSERT INTO gold.monthly_customer_metrics
                SELECT 
                    c.customer_id,
                    TO_CHAR(t.transaction_date, 'YYYY-MM') AS year_month,
                    COUNT(*) AS total_transactions,
                    SUM(t.amount) AS total_volume,
                    AVG(t.amount) AS avg_transaction_amount,
                    MAX(t.amount) AS max_transaction_amount,
                    COUNT(DISTINCT t.account_to) AS distinct_counterparties,
                    COUNT(CASE WHEN t.is_suspicious_flag = 1 THEN 1 END) AS suspicious_transactions,
                    COUNT(CASE WHEN t.amount >= 50000 THEN 1 END) AS high_value_transactions,
                    ROUND(COUNT(*)::NUMERIC / GREATEST(1, COUNT(DISTINCT t.transaction_date)), 2) AS txn_frequency,
                    AVG(c.risk_score) AS risk_score_avg,
                    NOW() AS loaded_at
                FROM silver.customers c
                JOIN silver.transactions t 
                    ON c.customer_id = t.customer_from
                WHERE t.transaction_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months')
                GROUP BY c.customer_id, TO_CHAR(t.transaction_date, 'YYYY-MM')
                ORDER BY c.customer_id, year_month DESC;"""

        cursor = conn.cursor()
        print(sql)
        cursor.execute(sql)
        conn.commit()
    
    [daily_account_summary(),customer_risk_profile(),monthly_customer_metrics()]


create_gold_dag()
