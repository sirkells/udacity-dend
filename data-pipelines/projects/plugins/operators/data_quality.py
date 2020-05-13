from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id,
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Starting Data quality check for tables")
        for table in self.tables:
            count = f"SELECT COUNT(*) FROM {table}"
            rows = redshift.get_records(count)
            table_is_empty = len(rows) < 1 or len(rows[0]) < 1 or rows[0][0] < 1
            
            if table_is_empty:
                self.log.error(f"Data quality check has failed because {table} table is empty")
                raise ValueError(f"Data quality check has failed because {table} table is empty")
            self.log.info(f"Data quality on {table} table has passed with {rows[0][0]} records")