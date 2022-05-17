from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str = None,
                 tables: List[str] = None,
                 sql_query: str = None,
                 *args, **kwargs) -> None:

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.sql_query = sql_query

    def execute(self, context):
        redshift_hook_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.tables:
            for table in self.tables:

                self.log.info(f"Starting test for table : {table}")
                records = redshift_hook_conn.get_records(f"select count(*) from {table};")

                if len(records) < 1:
                    self.log.error(f"Test failed: No data found in table {table}")
                    raise ValueError(f"Test failed: No data found in table: {table}")
                self.log.info(f"Test passed for table : {table}")
        if self.sql_query:
            self.log.info("Running test query ")
            records = redshift_hook_conn.get_records(self.sql_query)
            if len(records) < 1:
                self.log.error(f"Test failed: No rows returned for query")
                raise ValueError(f"Test failed: No rows returned for query")
            self.log.info(f"Test passed. Query returned some rows.")
