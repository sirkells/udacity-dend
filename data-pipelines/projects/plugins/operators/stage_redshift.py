from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to copy data from S3 to Redshift.
    Keyword arguments:
        redshift_conn_id: AWS Redshift connection ID
        aws_credentials_id : S3 credentials
        target_table       : Redshift target table name
        s3_bucket          : S3 bucket name
        s3_key             : S3 key
        file_format        : input data file format
        delimiter          : delimiter for CSV data (optional)
        ignore_headers     : ignore headers in CSV data
        execution_date     : context var task date
    Return:
        copies staging data into Redshift
    """
    ui_color = '#358140'
    template_fields = ("s3_key")
    csv_sql_template = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            IGNOREHEADER '{}'
            DELIMITER '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            {} 'auto' 

        """
     json_sql_template = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            {} 'auto' 
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 delimeter="",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.delimeter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        """
            Copy data from S3 buckets to redshift cluster into staging tables.
                - redshift_conn_id: redshift cluster connection
                - aws_credentials_id: AWS connection
                - table: redshift cluster table name
                - s3_bucket: S3 bucket name holding source data
                - s3_key: S3 key files of source data
                - file_format: source file format - options JSON, CSV
        """
        aws = AwsHook(self.aws_credentials_id)
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Deleting data from Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")

        path = "s3://{}".format(self.s3_bucket)
        if self.execution_date:
            # get date
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            path = [path, str(year), str(month), str(day)]
            path = '/'.join()
        path = path + '/' + self.s3_key

        if self.file_format == 'JSON':
            sql = StageToRedshiftOperator.json_sql_template.format(
                self.table,
                path,
                credentials.access_key,
                credentials.secret_key,
                self.file_format
                
            )
         elif self.file_format == 'CSV':
            sql = StageToRedshiftOperator.csv_sql_template.format(
                self.table,
                path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter,
                self.file_format
                
            )
         else:
            self.log.info('File Format not supported. Supported: JSON and CSV')
            pass
        
        redshift.run(sql)

        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")






