from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_fact_sql_template = """
    INSERT INTO {dest_table}
    ({select_sql})
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                dest_table="",
                select_sql="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dest_table=dest_table
        self.select_sql=select_sql

    def execute(self, context):
        self.log.info(f'LoadFactOperator dest_table: {self.dest_table}')

        redshift_hook = PostgresHook(self.redshift_conn_id)
        formatted_insert_sql = (LoadFactOperator
                                .insert_fact_sql_template
                                .format(dest_table=self.dest_table,
                                        select_sql=self.select_sql))

        redshift_hook.run(formatted_insert_sql)
