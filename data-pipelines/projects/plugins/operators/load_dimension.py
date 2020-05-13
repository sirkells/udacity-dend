from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator to insert data into dimensional tables from staging events and song data
    With the truncate-insert method to empty target tables prior to load.
    Keyword arguments:
        redshift_conn_id: AWS Redshift connection ID
        query       : sql query to insert data
        table: dimension table
        insert_mode: append or truncate_insert
    Return:
        insert data into dimensions table in Redshift
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query="",
                 table="",
                 insert_mode="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.table = table
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Inserting data into {self.table} table")
        if self.insert_mode == "truncate_insert":
            redshift.run(f"TRUNCATE TABLE {self.table}")
        sql = self.query.format(self.table)
        redshift.run(sql)
        self.log.info(f"Success: {self.task_id}")
