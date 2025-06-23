#!/usr/bin/env python3
import datetime
import os
from airflow import DAG

# from airflow.providers.postgres.operators.postgres import PostgresOperator
# 将 PostgresOperator 替换为 TacnodeOperator
from airflow_tacnode import TacnodeOperator, ISOLATION_LEVEL_READ_COMMITTED


with DAG(
    dag_id="pgtest",
    start_date=datetime.datetime(2025, 6, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    pid = os.getpid()
    op0 = TacnodeOperator(
        task_id="test_isolation_level_default",
        postgres_conn_id="m2",
        sql=f"""
INSERT INTO logs(pid, isolation_level, batch_id) VALUES(pg_backend_pid(), current_setting('transaction_isolation')::text, {pid}::text);
""",
    )


    op1 = TacnodeOperator(
        task_id="test_isolation_level_serializable",
        postgres_conn_id="m2",
        isolation_level=ISOLATION_LEVEL_READ_COMMITTED,
        sql=f"""
INSERT INTO logs(pid, isolation_level, batch_id) VALUES(pg_backend_pid(), current_setting('transaction_isolation')::text, {pid}::text);
""",
    )


if __name__ == '__main__':
    dag.cli()
