from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import os
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator


aws_conn_id = 'aws-conn' 
user_input_year,user_input_month='',''
user_input_year= Variable.get("emr_dag_year_month",deserialize_json=True)['year']
user_input_month= Variable.get("emr_dag_year_month",deserialize_json=True)['month']
if user_input_year!='' and user_input_month!='':
    data_year_string=user_input_year
    data_month_string=user_input_month
else:
    current_date=datetime.now().date()
    data_date=current_date- relativedelta(months=3)
    data_year_string=str(data_date.year)
    if data_date.month<10:
        data_month_string="0"+str(data_date.month)
    else:
        data_month_string=str(data_date.month)


lambda_payload={'year':data_year_string,'month':data_month_string}
lambda_payload = json.dumps(lambda_payload).encode('utf-8')





# Define the EMR steps (for running notebooks)
emr_steps = [
    {
        'Name': 'RunNotebookStep',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                "--class","org.apache.spark.examples.SparkPi",
                's3://emr-notebooks-nyctrip/notebooks/load_dw_bucket.py',
                data_year_string,
                data_month_string
            ],
        },
    }
]

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 6),
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
    'email': ['ashil2495@csu.fullerton.edu'],
    'email_on_success':True,
    'email_on_failure':True,
    'email_on_retry':False
}

dag = DAG(
    'NYC_Taxi_Analytics_DAG',
    default_args=default_args,
    description='DAG to create EMR cluster, run notebooks, and terminate cluster',
    schedule_interval=None,  # Set to None to trigger manually or via external trigger
    catchup=False
)

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    aws_conn_id=aws_conn_id,
    region_name='us-east-1',
    emr_conn_id='aws_emr_conn',
       job_flow_overrides={
        'Name': 'MyEMRCluster',
        'Instances': {
            'Ec2SubnetId': 'subnet-034487025a1a54f8b',
            'InstanceGroups': [
                {
                    'Name': 'MasterNode',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': 'CoreNodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                }
            ],
            'Ec2KeyName': 'tripdata_emr_key_pair',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False
        },
        'ReleaseLabel': 'emr-6.5.0',
        'ServiceRole': 'emr_default',
        'JobFlowRole': 'AmazonEMR-InstanceProfile-20231219T093914',
        'VisibleToAllUsers': True,
        'Applications': [
            {'Name': 'Livy'},
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'JupyterEnterpriseGateway'}
        ],
        'EbsRootVolumeSize': 15
    },
    dag=dag,  
)


wait_for_emr_cluster = EmrJobFlowSensor(
    task_id='wait_for_emr_cluster',
    aws_conn_id=aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster',key='emr_cluster')['job_flow_id'] }}",
    target_states=['WAITING'],
    timeout=7200,
    poke_interval=30,
    dag=dag, 
)



add_notebook_step = EmrAddStepsOperator(
    task_id='add_notebook_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster',key='emr_cluster')['job_flow_id'] }}",
    aws_conn_id=aws_conn_id,
    steps=emr_steps,
    dag=dag,   
)




wait_for_notebook_completion = EmrStepSensor(
    task_id='wait_for_notebook_completion',
    aws_conn_id=aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster',key='emr_cluster')['job_flow_id'] }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_notebook_step',key='return_value')[0] }}",
    target_states=['COMPLETED'],
    timeout=7200,
    poke_interval=30,
    soft_fail=True,
    dag=dag, 
)




terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster',key='emr_cluster')['job_flow_id'] }}",
    aws_conn_id=aws_conn_id,
    dag=dag,
)



def invoke_lambda_function():
    lambda_hook = LambdaHook(aws_conn_id=aws_conn_id)
    response=lambda_hook.invoke_lambda(
        function_name='download_source_data',
        invocation_type='RequestResponse',
        log_type=None,
        qualifier=None,
        client_context=None,
        payload=lambda_payload,
    )

invoke_lambda = PythonOperator(
    task_id='invoke_lambda',
    python_callable=invoke_lambda_function,
    dag=dag,
)

S3ListOperatorgreen = S3ListOperator(
    task_id='S3ListOperatorgreen',
    bucket='processed-trip-data',
    prefix='Fact/green/'+ data_year_string+'/'+data_month_string+'/',
    delimiter='/',
    aws_conn_id=aws_conn_id, 
    dag=dag
)

S3ListOperatoryellow = S3ListOperator(
    task_id='S3ListOperatoryellow',
    bucket='processed-trip-data',
    prefix='Fact/yellow/'+ data_year_string+'/'+data_month_string+'/',
    delimiter='/',
    aws_conn_id=aws_conn_id, 
    dag=dag
)

def save_files_to_xcom_green(**kwargs):
    ti = kwargs['ti']
    s3_file_path_green= ti.xcom_pull(task_ids='S3ListOperatorgreen')[1]
    return os.path.basename(s3_file_path_green)

def save_files_to_xcom_yellow(**kwargs):
    ti = kwargs['ti']
    s3_file_path_yellow = ti.xcom_pull(task_ids='S3ListOperatoryellow')[1]
    return os.path.basename(s3_file_path_yellow) 

save_files_task_green = PythonOperator(
    task_id='save_files_task_green',
    python_callable=save_files_to_xcom_green,
    provide_context=True,
    dag=dag
)

save_files_task_yellow = PythonOperator(
    task_id='save_files_task_yellow',
    python_callable=save_files_to_xcom_yellow,
    provide_context=True,
    dag=dag
)

load_files_to_redshift=PostgresOperator(
    task_id='load_files_to_redshift',
    postgres_conn_id='redshift_conn',
    sql='''
    CALL sp_insert_into_fact_green('{{params.data_year_string}}', '{{params.data_month_string}}','{{ti.xcom_pull(task_ids="save_files_task_green", key="return_value")}}');
    CALL sp_insert_into_fact_yellow('{{params.data_year_string}}', '{{params.data_month_string}}','{{ti.xcom_pull(task_ids="save_files_task_yellow", key="return_value")}}');
    ''',
    params={'data_year_string': data_year_string, 'data_month_string': data_month_string},
    dag=dag
    )


# Set task dependencies
invoke_lambda>>create_emr_cluster >> wait_for_emr_cluster >> add_notebook_step >> \
wait_for_notebook_completion >> terminate_emr_cluster >> S3ListOperatorgreen >> S3ListOperatoryellow \
>> save_files_task_green >> save_files_task_yellow >> load_files_to_redshift


if __name__ == "__main__":
    dag()