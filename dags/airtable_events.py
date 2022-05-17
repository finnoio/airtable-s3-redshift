from datetime import datetime, timedelta
import json
import io

import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from redshift_operator import RedshiftMultipleQueriesOperator
from load_increment_operator import LoadIncrementS3ToRedshift, LoadIncrementToTarget
from data_quality_operator import DataQualityOperator

from plugins.helpers.sql import SqlQueries
import src.airtable_module as airtable



def extract_events(run_dt) -> None:
    """
    Extract web and app events from airtable to raw zone in S3
    :param run_dt: date of dag run
    """
    hook = S3Hook('s3_conn')
    from_dt = datetime.strftime(datetime.strptime(run_dt, '%Y-%m-%d') - timedelta(days=1), '%Y-%m-%d')
    app_events = airtable.get_events_by_dt_range('App events', from_dt, run_dt)
    web_events = airtable.get_events_by_dt_range('Web events', from_dt, run_dt)
    for filename, events in {"app_events": app_events, "web_events": web_events}.items():
        hook.load_bytes(bytes(json.dumps(events).encode('UTF-8')),
                        key=f'nikolay/raw-events/dt={from_dt}/{filename}.json',
                        bucket_name='luko-data-eng-exercice',
                        replace=True)


def process_events(run_dt) -> None:
    """
    Get web and app events from raw zone in S3,
    transform & concatenate them and send result parquet file to processed zone in S3
    :param run_dt: date of dag run
    """
    hook = S3Hook('s3_conn')
    from_dt = datetime.strftime(datetime.strptime(run_dt, '%Y-%m-%d') - timedelta(days=1), '%Y-%m-%d')
    app_events = json.loads(hook.read_key(key=f'nikolay/raw-events/dt={from_dt}/app_events.json',
                                          bucket_name='luko-data-eng-exercice'))
    web_events = json.loads(hook.read_key(key=f'nikolay/raw-events/dt={from_dt}/web_events.json',
                                          bucket_name='luko-data-eng-exercice'))
    # transform app events
    app_events_df = pd.json_normalize([event['fields'] for event in app_events['records']])

    # extract properties
    app_event_properties_df = pd.json_normalize(app_events_df['EVENT_PROPERTIES'].apply(json.loads)).add_prefix('ep_')
    event_properties_columns = ['ep_questionType', 'ep_session_id', 'ep_screen', 'ep_from_background', 'ep_label']

    app_event_properties_df = app_event_properties_df[
        app_event_properties_df.columns.intersection(event_properties_columns)]
    app_event_properties_df = pd.concat([pd.DataFrame(columns=event_properties_columns), app_event_properties_df])

    app_events_df = app_events_df.drop(columns=['EVENT_PROPERTIES'])
    app_events_df['event_source'] = 'APP'
    transformed_app_events_df = pd.concat([app_events_df, app_event_properties_df], axis=1)

    # Transform web_events
    web_events_df = pd.json_normalize([event['fields'] for event in web_events['records']])
    web_events_meta_df = pd.json_normalize(web_events_df['METADATA'].apply(json.loads)).add_prefix('meta_')
    web_events_df = web_events_df.drop(columns=['METADATA'])
    web_events_df['event_source'] = 'WEB'
    transformed_web_events_df = pd.concat([web_events_df, web_events_meta_df], axis=1)

    # Concat app and web events for the result dataframe
    transformed_events_df = pd.concat([transformed_app_events_df, transformed_web_events_df], ignore_index=True)

    # Format column names
    transformed_events_df.columns = transformed_events_df.columns.str.lower()
    transformed_events_df.rename(columns=lambda x: x.replace('.', '_'), inplace=True)

    transformed_events_df['created_at'] = pd.to_datetime(transformed_events_df['created_at'])

    # Write parquet to processed zone in S3
    parquet_buffer = io.BytesIO()
    transformed_events_df.to_parquet(parquet_buffer, index=False)
    hook.load_bytes(parquet_buffer.getvalue(),
                    key=f'nikolay/processed-events/dt={from_dt}/events.parquet',
                    bucket_name='luko-data-eng-exercice',
                    replace=True)


with DAG(
    dag_id='airtable_events',
    schedule_interval='15 0 * * *',
    start_date=datetime(2021, 5, 2),
    end_date=datetime(2021, 6, 1),
    max_active_runs=1,
    catchup=True
) as dag:

    extract_events_task = PythonOperator(
        task_id='extract_events',
        python_callable=extract_events,
        op_kwargs={
            'app_events_airtable': 'App events',
            'web_events_airtable': 'Web events',
            'run_dt': '{{ ds }}'
        }
    )

    process_events_task = PythonOperator(
        task_id='process_events',
        python_callable=process_events,
        op_kwargs={
            'run_dt': '{{ ds }}'
        }
    )

    load_data_to_event_inc_task = LoadIncrementS3ToRedshift(
        task_id='load_to_increment_table',
        redshift_conn_id='redshift_conn_id',
        s3_conn_id='s3_conn',
        increment_table='nikolay.event_inc',
        s3_bucket='luko-data-eng-exercice',
        s3_key='nikolay/processed-events/dt={}/events.parquet'
    )

    check_data_in_events_inc_task = DataQualityOperator(
        task_id='check_data_in_events_inc',
        redshift_conn_id='redshift_conn_id',
        tables=['nikolay.event_inc']
    )

    load_data_to_event_task = LoadIncrementToTarget(
        task_id='load_data_to_event',
        redshift_conn_id='redshift_conn_id',
        insert_query=SqlQueries.insert_increment_to_event,
        target_table='nikolay.event'
    )

    check_data_in_event_task = DataQualityOperator(
        task_id='check_data_in_event',
        redshift_conn_id='redshift_conn_id',
        sql_query=SqlQueries.check_increment_loaded_to_target.format('nikolay.event', 'nikolay.event')
    )

    load_data_to_event_sequence_task = RedshiftMultipleQueriesOperator(
        task_id='load_data_to_event_sequence',
        redshift_conn_id='redshift_conn_id',
        sql_queries=[
            'TRUNCATE nikolay.event_sequence;',
            SqlQueries.insert_events_to_event_sequence
        ]
    )

    check_data_in_event_sequence_task = DataQualityOperator(
        task_id='check_data_in_event_sequence',
        redshift_conn_id='redshift_conn_id',
        tables=['nikolay.event_sequence']
    )

extract_events_task >> process_events_task >> load_data_to_event_inc_task >> check_data_in_events_inc_task >> load_data_to_event_task >> check_data_in_event_task >> load_data_to_event_sequence_task >> check_data_in_event_sequence_task