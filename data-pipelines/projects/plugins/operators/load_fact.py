from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator to insert data into Facts Table from staging.
    Keyword arguments:
        redshift_conn_id: AWS Redshift connection ID
        aws_credentials_id : S3 credentials
        query       : sql query to insert data
     
    Return:
        insert data into Facts Table in Redshift
    """

    ui_color = '#F98866'

    @apply_defaults
        def __init__(self,
                     aws_credentials_id="",
                     redshift_conn_id="",
                     query="",
                     *args, **kwargs):

            super(LoadFactOperator, self).__init__(*args, **kwargs)
            self.aws_credentials_id = aws_credentials_id,
            self.redshift_conn_id = redshift_conn_id,
            self.query = query,

        def execute(self, context):
            redshift = PostgresHook(self.redshift_conn_id)
            self.log.info("Inserting data into Facts Table in Redshift")
            redshift.run(str(self.query))
            self.log.info('Success: Inserted data into Facts Table')