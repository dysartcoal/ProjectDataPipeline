from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                tests=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests=tests

    def execute(self, context):
        self.log.info(f'DataQualityOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for test in self.tests:
            sql = test['sql']
            assertion = test['assertion']
            error_message = test['error_message']
            log_message = test['log_message']

            self.log.info(f'Test SQL: {sql}')
            records = redshift_hook.get_records(sql)
            success = assertion(records)

            if not success:
                raise ValueError(error_message)
            self.log.info(log_message)
