from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                primary_key="",
                not_nulls=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.primary_key = primary_key
        self.not_nulls = not_nulls

    def execute(self, context):
        self.log.info(f'DataQualityOperator table: {self.table}')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Check that data exists
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. {} returned no results".format(self.table))
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. {} contained 0 rows".format(self.table))
        self.log.info(f"Data quality check for data on table {self.table} passed with {records[0][0]} records")

        # Check that primary_key is unique
        if self.primary_key != "":
            check_primary_key_sql = """SELECT {}, COUNT({}) AS pkey_count
                    FROM {}
                    GROUP BY {}
                    HAVING pkey_count > 1
                    """
            records = redshift_hook.get_records(check_primary_key_sql.format(self.primary_key,
                                                                            self.primary_key,
                                                                            self.table,
                                                                            self.primary_key))
            if len(records) > 0 and len(records[0]) > 0:
                raise ValueError("Data quality check failed. {} has duplicates in {} column"
                                .format(self.table, self.primary_key))
            self.log.info(f"Data quality check for primary_key {self.primary_key} " +
                        f"on table {self.table} passed with no duplicates")

        # Check that there are no nulls in the not null columns
        if self.not_nulls != None and len(self.not_nulls) > 0:
            check_for_nulls_sql = "SELECT count(*) FROM {} WHERE {} is NULL"
            for col in self.not_nulls:
                records = redshift_hook.get_records(check_for_nulls_sql.format(self.table, col))
                if len(records) > 0 and len(records[0]) == 1 and records[0][0] > 0:
                    num_records = records[0][0]
                    raise ValueError("Data quality check failed. {} contains {} null(s) in col {}"
                                    .format(self.table, num_records, col))
                self.log.info(f"Data quality check passed.  Zero nulls in col {col} on table {self.table}")
