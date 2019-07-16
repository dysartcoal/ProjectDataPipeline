from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'


    truncate_dim_sql_template = """
    TRUNCATE {dest_table}
    """

    insert_dim_sql_template = """
    INSERT INTO {dest_table}
    ({select_sql})
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                dest_table="",
                select_sql="",
                append=True,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dest_table=dest_table
        self.select_sql=select_sql
        self.append=append

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator dest_table: {self.dest_table}, append is {self.append}')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        formatted_truncate_sql = (LoadDimensionOperator
                                .truncate_dim_sql_template
                                .format(dest_table=self.dest_table))
        formatted_insert_sql = (LoadDimensionOperator
                                .insert_dim_sql_template
                                .format(dest_table=self.dest_table,
                                        select_sql=self.select_sql))

        if not self.append:
            redshift_hook.run(formatted_truncate_sql)
        redshift_hook.run(formatted_insert_sql)
