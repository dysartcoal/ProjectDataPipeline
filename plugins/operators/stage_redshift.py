from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {}
        {}
        {}
        {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 data_format="",
                 data_format_args=None,
                 data_format_kwargs=None,
                 file_compression="",
                 data_conversion_args=None,
                 data_conversion_kwargs=None,
                 data_load_args=None,
                 data_load_kwargs=None,
                 del_existing=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.data_format = data_format
        self.data_format_args = data_format_args or []
        self.data_format_kwargs = data_format_kwargs or {}
        self.file_compression = file_compression
        self.data_conversion_args = data_conversion_args or []
        self.data_conversion_kwargs = data_conversion_kwargs or {}
        self.data_load_args = data_load_args or []
        self.data_load_kwargs = data_load_kwargs or {}
        self.del_existing = del_existing


    def execute(self, context):
        self.log.info(f'StageToRedshiftOperator table: {self.table} {self.s3_bucket}/{self.s3_key}')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.del_existing:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))


        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        data_format_options = ''
        data_conversion = ''
        data_load_ops = ''
        if self.data_format_args != None:
            if type(self.data_format_args) is str:
                data_format_options += self.data_format_args
            else:
                data_format_options += ' '.join(self.data_format_args)
            data_format_options += ' '
        if self.data_format_kwargs != None:
            data_format_options += ' '.join([k + ' ' + str(v) for k,v in self.data_format_kwargs.items()])
        if self.data_conversion_args != None:
            if type(self.data_conversion_args) is str:
                data_conversion += self.data_conversion_args
            else:
                data_conversion += ' '.join(self.data_conversion_args)
            data_conversion += ' '
        if self.data_conversion_kwargs != None:
            data_conversion += ' '.join([k + ' ' + str(v) for k,v in self.data_conversion_kwargs.items()])
        if self.data_load_args != None:
            if type(self.data_load_args) is str:
                data_load_ops += self.data_load_args
            else:
                data_load_ops += ' '.join(self.data_load_args)
            data_load_ops += ' '
        if self.data_load_kwargs != None:
            data_load_ops += ' '.join([k + ' ' + str(v) for k,v in self.data_load_kwargs.items()])

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.data_format,
            data_format_options,
            self.file_compression,
            data_conversion,
            data_load_ops
        )
        redshift.run(formatted_sql)
