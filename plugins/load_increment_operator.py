from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from helpers.sql import SqlQueries

class LoadIncrementS3ToRedshift(BaseOperator):
    ui_color = '#358140'

    copy_sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT as parquet
    """

    def __init__(self,
                 redshift_conn_id='',
                 s3_conn_id='',
                 increment_table='',
                 s3_bucket='',
                 s3_key='',
                 *args, **kwargs):
        super(LoadIncrementS3ToRedshift, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_conn_id = s3_conn_id
        self.increment_table = increment_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        self.log.info('Start loading increment...')
        s3_hook = S3Hook(self.s3_conn_id)
        credentials = s3_hook.get_credentials()
        redshift_hook_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        increment_date = datetime.strftime(datetime.strptime(context['ds'], '%Y-%m-%d') - timedelta(days=1), '%Y-%m-%d')

        self.log.info(f'Clearing data from increment table {self.increment_table}')
        redshift_hook_conn.run(f'TRUNCATE {self.increment_table}')

        self.log.info('Copying data from S3 to Redshift increment table')
        key = self.s3_key.format(increment_date)
        s3_path = f's3://{self.s3_bucket}/{key}'
        formatted_sql_stm = LoadIncrementS3ToRedshift.copy_sql_template.format(
            self.increment_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key
        )
        redshift_hook_conn.run(formatted_sql_stm)
        self.log.info('Data copying finished!')


class LoadIncrementToTarget(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str = None,
                 insert_query: str = None,
                 target_table: str = None,
                 *args, **kwargs) -> None:

        super(LoadIncrementToTarget, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_query = insert_query
        self.target_table = target_table

    def execute(self, context):
        redshift_hook_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Cleaning target table {self.target_table} from increment possibly loaded in previous runs.')
        redshift_hook_conn.run(
            SqlQueries.delete_increment_from_target.format(self.target_table, context['ds'], context['ds']))
        self.log.info(f'Cleaning finished.')

        self.log.info(f'Insert increment to target table {self.target_table}.')
        redshift_hook_conn.run(self.insert_query.format(self.target_table, self.target_table))
        self.log.info('Increment successfully loaded.')

