from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

def initializer(**kwargs):
    src_sys_id = "src_sys_id_placeholder"
    ast_id = "ast_id_placeholder"
    env = "env_placeholder"
    instance_id = datetime.now().strftime("%Y%m%d%H%M%S")
    exec_id = f"{src_sys_id}_{ast_id}_{instance_id}"
    # job_name_args = f"df-fmwrk-data-publish-{env}"
    task_instance = kwargs['task_instance']

    ingestion_glue_job = f"df-fmwrk-data-ingestion-{env}"
    publish_glue_job = f"df-fmwrk-data-publish-{env}"
    dq_glue_job = f"df-fmwrk-data-quality-check-{env}"
    task_instance.xcom_push(key="ingestion_glue_job", value=ingestion_glue_job)
    task_instance.xcom_push(key="publish_glue_job", value=publish_glue_job)
    task_instance.xcom_push(key="dq_glue_job", value=dq_glue_job)

    code_bucket = "df-fmwrk-code-us-east-2"
    framework_script_location = f"s3://{code_bucket}/{env}/aws-data-fabric-framework/src/"
    task_instance.xcom_push(key="framework_script_location", value=framework_script_location)

    task_instance.xcom_push(key="src_sys_id", value=src_sys_id)
    task_instance.xcom_push(key="ast_id", value=ast_id)
    task_instance.xcom_push(key="exec_id", value=exec_id)
    # task_instance.xcom_push(key="job_name_args", value=job_name_args)

schedule = "schedule_placeholder"
yesterday = datetime.combine(datetime.today(), datetime.min.time())
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date' : yesterday
}

dag =  DAG(
    dag_id = "dag_id_placeholder",
    default_args = default_args,
    schedule_interval=schedule,    
    catchup=False,
    is_paused_upon_creation=False
)

t1 = PythonOperator(
    task_id='start',
    python_callable=initializer,
    provide_context=True,
    dag = dag
)

t2 = GlueJobOperator(
    task_id = "data_ingestion",
    job_name = "{{ task_instance.xcom_pull(task_ids='start', key='ingestion_glue_job')}}",
    region_name = "us-east-2",
    script_location = "{{ task_instance.xcom_pull(task_ids='start', key='framework_script_location')}}",
    num_of_dpus = 1,
    script_args = {
        "--source_id" : "{{ task_instance.xcom_pull(task_ids='start', key='src_sys_id')}}",
        "--asset_id" : "{{ task_instance.xcom_pull(task_ids='start', key='ast_id')}}",
        "--exec_id" : "{{ task_instance.xcom_pull(task_ids='start', key='exec_id')}}"
        },
    dag = dag
    )

t3 = GlueJobOperator(
    task_id = "data_quality",
    job_name = "{{ task_instance.xcom_pull(task_ids='start', key='dq_glue_job')}}",
    region_name = "us-east-2",
    script_location = "{{ task_instance.xcom_pull(task_ids='start', key='framework_script_location')}}",
    num_of_dpus = 1,
    script_args = {
        "--source_id" : "{{ task_instance.xcom_pull(task_ids='start', key='src_sys_id')}}",
        "--asset_id" : "{{ task_instance.xcom_pull(task_ids='start', key='ast_id')}}",
        "--exec_id" : "{{ task_instance.xcom_pull(task_ids='start', key='exec_id')}}"
        },
    dag = dag
    )

t4 = GlueJobOperator(
    task_id = "data_publish",
    job_name = "{{ task_instance.xcom_pull(task_ids='start', key='publish_glue_job')}}",
    region_name = "us-east-2",
    script_location = "{{ task_instance.xcom_pull(task_ids='start', key='framework_script_location')}}",
    num_of_dpus = 1,
    script_args = {
        "--source_id" : "{{ task_instance.xcom_pull(task_ids='start', key='src_sys_id')}}",
        "--asset_id" : "{{ task_instance.xcom_pull(task_ids='start', key='ast_id')}}",
        "--exec_id" : "{{ task_instance.xcom_pull(task_ids='start', key='exec_id')}}"
        # "--JOB_NAME" : "{{ task_instance.xcom_pull(task_ids='start', key='job_name_args')}}"
        },
    dag = dag
    )


t5 = DummyOperator(
    task_id='end',
    dag = dag
)

t1 >> t2 >> t3 >> t4 >> t5
