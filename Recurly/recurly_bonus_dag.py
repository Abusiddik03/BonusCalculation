############################################################################################
## Business Area : Bouns Calculation (Using Recurly)                					####
## Date Changed  : 04-03-2020                                                           ####
## Report Name   : Recurly Bouns Calculation DAG                                        ####
## Written By.   : Manish Pansari                                                       ####
############################################################################################

from airflow import DAG
from airflow.operators import BashOperator, DummyOperator
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator

default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'provide_context': True,
                'start_date': datetime(2020, 4, 3, 0, 0),
                'email': ['mpansari@godaddy.com'],
                'email_on_failure': True,
                'email_on_retry': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }

#schedule_interval = '30 6 * * *'
dag_name = 'recurly_bonus_calculation'
HOME_DIR = "/home/025d3777f4b1ecm/BonusCalculation_Recurly"

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

recurly_to_sql_server_copy = BashOperator(task_id='recurly_to_sql_server_copy',
                                            bash_command='sh -x {0}/recurly_exec.sh '.format(HOME_DIR),
                                            dag=dag)

success_notification = EmailOperator(to=['mpansari@godaddy.com',"skhedikar@godaddy.com"], task_id='success_notification',
                                     subject='[Success:] Recurly Bonus Calculation DAG for {{ ds }}',
                                     html_content='Successfully ran the DAG for Recurly Bonus Calculation at {{ ds }}.',
                                     dag=dag)
end = DummyOperator(
    task_id='end',
    dag=dag,
)
recurly_to_sql_server_copy.set_upstream(start)
success_notification.set_upstream(recurly_to_sql_server_copy)
end.set_upstream(success_notification)