from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        
    def execute(self, context):
        self.log.info('DataQualityOperator Execution')
        redshift  = PostgresHook(self.redshift_conn_id)
        total_success = 0
        total_failure = 0
        
        if self.dq_checks is not None:
            for test in self.dq_checks:
                records = redshift.get_records(test['check_sql'])
                if records[0][0] == test['expected_result']:
                    self.log.info(
                        f"""+++ Data quality check succeeded: {test['description']}"""
                    )
                    total_success+=1
                else:   
                    self.log.info(
                        f"""--- Data quality check failed: {test['description']}"""
                    )
                    total_failure+=1
            self.log.info(f"""
            
            ***************DATA--QUALITY--CHECK--SUMMARY*****************
            
            Total test success   = {total_success}
            Total test failure   = {total_failure}
            Average Test Success = {round((total_success/(total_success+total_failure))*100, 2)}%
            
            *************************************************************
            
            """)
            
            
            