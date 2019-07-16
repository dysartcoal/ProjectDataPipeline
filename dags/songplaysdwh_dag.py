from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

s3_key_song_data = 'song_data/A/A/A'
s3_key_log_data = 'log_data/{execution_date.year}/{execution_date.month}/2018-11-16-events.json'
retry_delay = timedelta(minutes=1)

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 30, 23, 0, 0),
    'end_date': datetime(2018, 11, 30, 23, 0, 0),
    'retries': 3,
    'retry_delay': retry_delay,
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('songplaysdwh_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=timedelta(hours=1)
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key=s3_key_log_data,
    region='us-west-2',
    data_format = 'json',
    data_format_args = "'s3://udacity-dend/log_json_path.json'",
    data_format_kwargs = None,
    file_compression = '',
    data_conversion_args = None,
    data_conversion_kwargs = None,
    data_load_args = None,
    data_load_kwargs = {'STATUPDATE':'OFF'},
    del_existing = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key=s3_key_song_data,
    region='us-west-2',
    data_format = 'json',
    data_format_args = "'auto'",
    data_format_kwargs = None,
    file_compression = '',
    data_conversion_args = None,
    data_conversion_kwargs = None,
    data_load_args = None,
    data_load_kwargs = {'STATUPDATE':'OFF'},
    del_existing = True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    dest_table='songplays',
    select_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dest_table='users',
    select_sql=SqlQueries.user_table_insert,
    append=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dest_table='songs',
    select_sql=SqlQueries.song_table_insert,
    append=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dest_table='artists',
    select_sql=SqlQueries.artist_table_insert,
    append=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dest_table='time',
    select_sql=SqlQueries.time_table_insert,
    append=False
)

run_songplay_quality_checks = DataQualityOperator(
    task_id='Run_songplay_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    primary_key='playid',
    not_nulls=['start_time', 'userid']
)

run_user_quality_checks = DataQualityOperator(
    task_id='Run_user_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    primary_key='userid',
    not_nulls=None
)

run_song_quality_checks = DataQualityOperator(
    task_id='Run_song_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    primary_key='songid',
    not_nulls=['title']
)

run_artist_quality_checks = DataQualityOperator(
    task_id='Run_artist_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    primary_key='',
    not_nulls=['artistid']
)

run_time_quality_checks = DataQualityOperator(
    task_id='Run_time_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    primary_key='start_time',
    not_nulls=None
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> run_songplay_quality_checks
run_songplay_quality_checks >> load_user_dimension_table
run_songplay_quality_checks >> load_song_dimension_table
run_songplay_quality_checks >> load_artist_dimension_table
run_songplay_quality_checks >> load_time_dimension_table
load_user_dimension_table >> run_user_quality_checks
load_song_dimension_table >> run_song_quality_checks
load_artist_dimension_table >> run_artist_quality_checks
load_time_dimension_table >> run_time_quality_checks
run_user_quality_checks >> end_operator
run_song_quality_checks >> end_operator
run_artist_quality_checks >> end_operator
run_time_quality_checks >> end_operator
