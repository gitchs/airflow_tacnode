#!/usr/bin/env python3
import datetime
import os
from functools import wraps
from psycopg2.extensions import connection
from psycopg2.sql import SQL, Identifier
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from airflow import DAG


ISOLATION_LEVEL_READ_COMMITTED = 'READ COMMITTED'
ISOLATION_LEVEL_SERIALIZABLE = 'SERIALIZABLE'




class TacnodeHook(PostgresHook):
    def __init__(self, *args, isolation_level=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.isolation_level = None
        if isinstance(isolation_level, str):
            isolation_level = isolation_level.upper()
            if isolation_level not in [
                ISOLATION_LEVEL_READ_COMMITTED,
                ISOLATION_LEVEL_SERIALIZABLE,
            ]:
                raise RuntimeError('invalid isolation level')
            self.isolation_level = isolation_level

    def get_conn(self) -> connection:
        conn = super().get_conn()
        if self.isolation_level is not None:
            # alter session transaction isolation level
            cursor = conn.cursor()
            cursor.execute('''SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ''' + self.isolation_level)
            conn.commit()
            cursor.close()
        return conn


class TacnodeOperator(PostgresOperator):
    def __init__(self, *args, isolation_level=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.isolation_level = isolation_level

    def execute(self, context):
        self.hook = TacnodeHook(postgres_conn_id=self.postgres_conn_id, schema=self.database, isolation_level=self.isolation_level)
        if self.runtime_parameters:
            final_sql = []
            sql_param = {}
            for param in self.runtime_parameters:
                set_param_sql = f"SET {{}} TO %({param})s;"
                dynamic_sql = SQL(set_param_sql).format(Identifier(f"{param}"))
                final_sql.append(dynamic_sql)
            for param, val in self.runtime_parameters.items():
                sql_param.update({f"{param}": f"{val}"})
            if self.parameters:
                sql_param.update(self.parameters)
            if isinstance(self.sql, str):
                final_sql.append(SQL(self.sql))
            else:
                final_sql.extend(list(map(SQL, self.sql)))
            self.hook.run(final_sql, self.autocommit, parameters=sql_param)
        else:
            self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)
