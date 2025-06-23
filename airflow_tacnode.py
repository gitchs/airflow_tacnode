#!/usr/bin/env python3
from psycopg2.extensions import connection
from psycopg2.sql import SQL, Identifier
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook


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
        self.log.info('Executing: %s', self.sql)
        self.hook = TacnodeHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)
