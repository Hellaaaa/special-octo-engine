from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Start one day ago to allow triggering
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'mock_celery_dag',
    default_args=default_args,
    description='A simple DAG to run a task using a Docker image on Celery',
    schedule_interval='@once',  # Run once
    catchup=False,
) as dag:

    # Define the DockerOperator task
    run_docker_task = DockerOperator(
        task_id='run_mock_image_on_celery',
        image='localhost:5001/mock-image',  # Update this if the registry uses a different port
        api_version='auto',
        auto_remove=True,  # Remove the container when it finishes
        docker_url='unix://var/run/docker.sock',  # This is the default Docker socket
        network_mode='bridge',
    )

    run_docker_task