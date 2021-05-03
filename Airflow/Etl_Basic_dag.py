from __future__ import print_function
import logging
import airflow
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
dag = airflow.DAG(
    'Etl_Pyspark_trans_cluster',
    schedule_interval='@once',
    default_args=default_args,
    max_active_runs=1)

default_emr_settings = {"Name": "sales_job_flow",
                        "LogUri": "s3://8k-vigneshwar-de/server-feed/",
                        "ReleaseLabel": "emr-5.19.0",
                        "Instances": {
                            "InstanceGroups": [
                                {
                                    "Name": "Master nodes",
                                    "Market": "SPOT",
                                    "InstanceRole": "MASTER",
                                    "InstanceType": "m5.xlarge",
                                    "InstanceCount": 1
                                },
                                {
                                    "Name": "Slave nodes",
                                    "Market": "SPOT",
                                    "InstanceRole": "CORE",
                                    "InstanceType": "m5.xlarge",
                                    "InstanceCount": 1
                                }
                            ],
                            "Ec2KeyName": "vigneshwar-de.pem",
                            "KeepJobFlowAliveWhenNoSteps": True,
                            #'EmrManagedMasterSecurityGroup': 'sg-XXXXXXXXX',
                            #'EmrManagedSlaveSecurityGroup': 'sg-XXXXXXXXXX',
                            'Placement': {
                                'AvailabilityZone': 'us-east-1',
                            },
                        },
                        "BootstrapActions": [
                            {
                                'Name': 'copy config to local',
                                'ScriptBootstrapAction': {
                                    'Path': 's3://emr-pipeline-id-XXXX/BootStrap/copy_config.sh'
                                }
                            }
                        ],
                        "Applications": [
                            { 'Name': 'hadoop' },
                         { 'Name': 'spark' }
                        ],
                        "VisibleToAllUsers": True,
                        "JobFlowRole": "EMR_EC2_DefaultRole",
                        "ServiceRole": "EMR_DefaultRole",
                        "Tags": [
                            {
                                "Key": "app",
                                "Value": "analytics"
                            },
                            {
                                "Key": "environment",
                                "Value": "development"
                            }
                        ]
                        }


def issue_step():
    return [
        {
            'Name': 'spark-submit',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit', '--deploy-mode', 'cluster', 's3://8k-vigneshwar-de/etlPyspark.py'
                    , 'Weather_report_kaggle_2.csv', 'Weather_report_kaggle_op.parquet'

                ]
            }
        }
    ]


create_job_flow_task = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    job_flow_overrides=default_emr_settings,
    dag=dag
)

'''
run_step = issue_step('run_spark_job', ["spark-submit", "--deploy-mode", "client", "--master",
                               "yarn", "--py-files", "s3://emr-pipeline-id-XXX/code/dependencies.zip",
                               "s3://emr-pipeline-id-XXX/code/jobs/etl_job.py",
                               "/home/hadoop/etl_config.json"])

                               '''

run_step = issue_step()

add_step_task = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=run_step,
    dag=dag
)
watch_prev_step_task = EmrStepSensor(
    task_id='watch_prev_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)
terminate_job_flow_task = EmrTerminateJobFlowOperator(
    task_id='terminate_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule="all_done",
    dag=dag
)

create_job_flow_task >> add_step_task
add_step_task >> watch_prev_step_task
watch_prev_step_task >> terminate_job_flow_task
