from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

s3_key_song_data = 'song_data'
s3_key_log_data = 'log_data/{execution_date.year}/{execution_date.month}'
retry_delay = timedelta(minutes=5)

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

# Assertion function returns True when conditions met
def count_gt_zero(records):
    if len(records) < 1 or len(records[0]) < 1:
        return False
    num_records = records[0][0]
    if num_records < 1:
        return False
    return True

# Assertion function returns True when conditions met
def count_eq_zero(records):
    if len(records) > 0 and len(records[0]) > 0:
        num_records = records[0][0]
        if num_records > 0:
            return False
    return True

# Return test dict to enable testing for existence of data by DataQualityOperator
def get_data_exists_test(table=''):
    return {'sql': "SELECT COUNT(*) FROM {}".format(table),
            'assertion': count_gt_zero,
            'error_message': "Data quality check failed. {} contained 0 rows".format(table),
            'log_message':('Data quality check for data on table {} passed with > 0 records'
                            .format(table))}

# Return test dict to enable testing of primary key for uniqueness by DataQualityOperator
def get_primary_key_test(table='', primary_key=''):
    return {'sql': """SELECT COUNT(*) FROM
                            (SELECT COUNT({}) AS pkey_count
                            FROM {}
                            GROUP BY {}
                            HAVING pkey_count > 1) dup_pkeys
                            """.format(primary_key, table, primary_key),
            'assertion': count_eq_zero,
            'error_message': ("Data quality check failed. {} has duplicates in {} column"
                            .format(table, primary_key)),
            'log_message':("Data quality check for primary_key {} ".format(primary_key) +
                        "on table {} passed with no duplicates".format(table))}

# Return test dict to enable checking that there are no nulls for a column by DataQualityOperator
def get_not_nulls_test(table='', field=''):
    return {'sql': "SELECT count(*) FROM {} WHERE {} is NULL".format(table, field),
            'assertion': count_eq_zero,
            'error_message': ("Data quality check failed. {} contains null(s) in col {}"
                            .format(table, field)),
            'log_message':("Data quality check passed.  Zero nulls in col {} on table {}"
                        .format(field, table))}

# songplays tests
data_exists_test = get_data_exists_test(table='songplays')
primary_key_test = get_primary_key_test(table='songplays', primary_key='playid')
not_null_start_time_test = get_not_nulls_test(table='songplays', field='start_time')
not_null_userid_test = get_not_nulls_test(table='songplays', field='userid')
all_tests = [data_exists_test,
            primary_key_test,
            not_null_start_time_test,
            not_null_userid_test]

run_songplay_quality_checks = DataQualityOperator(
    task_id='Run_songplay_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tests=all_tests
)

# user tests
data_exists_test = get_data_exists_test(table='users')
primary_key_test = get_primary_key_test(table='users', primary_key='userid')
all_tests = [data_exists_test,
            primary_key_test]

run_user_quality_checks = DataQualityOperator(
    task_id='Run_user_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tests=all_tests
)

# song tests
data_exists_test = get_data_exists_test(table='songs')
primary_key_test = get_primary_key_test(table='songs', primary_key='songid')
not_null_test = get_not_nulls_test(table='songs', field='title')
all_tests = [data_exists_test,
            primary_key_test,
            not_null_test]

run_song_quality_checks = DataQualityOperator(
    task_id='Run_song_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tests=all_tests
)

# artist tests
data_exists_test = get_data_exists_test(table='artists')
not_null_test = get_not_nulls_test(table='artists', field='artistid')
all_tests = [data_exists_test,
            not_null_test]

run_artist_quality_checks = DataQualityOperator(
    task_id='Run_artist_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tests=all_tests
)

# time tests
data_exists_test = get_data_exists_test(table='time')
primary_key_test = get_primary_key_test(table='time', primary_key='start_time')
all_tests = [data_exists_test,
            primary_key_test]

run_time_quality_checks = DataQualityOperator(
    task_id='Run_time_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tests=all_tests
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
