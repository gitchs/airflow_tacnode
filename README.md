# Airflow Tacnode

## 功能概述
`TacnodeOperator` 是对 Airflow 原生 PostgreSQLOperator 的扩展，支持动态设置 PostgreSQL 的事务隔离级别。通过这一功能，可以更灵活地控制数据库操作的并发性和数据一致性。

## 支持的隔离级别
- Tacnode 默认使用最高的隔离级别 `SERIALIZABLE`, 确保每个事务看到的数据是一致的快照。
- 通过 `TacnodeOperator` 可以将 Session 的隔离级别修改为 `READ COMMITTED`， 较少不必要的事务冲突。


## 快速上手：如何使用 TacnodeOperator
1. 将文件 `airflow_tacnode.py` 的代码，复制到项目中。
2. 将原来使用 `PostgresqlOperator` 的类，修改为 `TacnodeOperator`，并指定隔离级别。


## 样例
```python

#!/usr/bin/env python3
import datetime
import os
from functools import wraps
from psycopg2.extensions import connection
from psycopg2.sql import SQL, Identifier
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
        task_id="test_isolation_level_serializable",
        postgres_conn_id="m2",
        isolation_level=ISOLATION_LEVEL_READ_COMMITTED, # 这里指定隔离级别
        sql=f"""
INSERT INTO logs(pid, isolation_level, batch_id) VALUES(pg_backend_pid(), current_setting('transaction_isolation')::text, {pid}::text);
""",
    )


if __name__ == '__main__':
    dag.cli()

```

