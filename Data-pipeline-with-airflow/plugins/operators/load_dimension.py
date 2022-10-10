from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 purge_table=True,
                 sql=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.purge_table = purge_table

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        if self.purge_table:
            self.log.info(f"Deleting data from {self.table}")
            redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info(f"LoadDimensionOperator table {self.table} execution")
        redshift.run(self.sql)

        