from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class RedshiftMultipleQueriesOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str = None,
                 sql_queries: List[str] = None,
                 *args, **kwargs) -> None:

        super(RedshiftMultipleQueriesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_queries = sql_queries

    def execute(self, context):
        redshift_hook_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for query in self.sql_queries:
            self.log.info(f'Running query ({self.sql_queries[:30]})')
            redshift_hook_conn.run(query.format(context['ds']))
        self.log.info('All queries completed')

