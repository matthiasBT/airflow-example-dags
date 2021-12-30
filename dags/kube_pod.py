from time import sleep

from airflow import DAG
from datetime import datetime, timedelta
from airflow import configuration as conf
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


namespace = conf.get('kubernetes', 'NAMESPACE')

# This will detect the default namespace locally and read the 
# environment namespace when deployed to Astronomer.
if namespace =='default':
    config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster=False
else:
    in_cluster=True
    config_file=None

dag = DAG('example_kubernetes_pod',
          schedule_interval='@once',
          default_args=default_args)


compute_resource = {'request_cpu': '200m', 'request_memory': '1Gi', 'limit_cpu': '200m', 'limit_memory': '1Gi'}


def lol():
    sleep(5)
    print("kek")


with dag:
    k = KubernetesPodOperator(
        namespace=namespace,
        image="hello-world",
        labels={"foo": "bar"},
        name="airflow-test-pod",
        task_id="task-one",
        in_cluster=in_cluster, # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-for-desktop', # is ignored when in_cluster is set to True
        config_file=config_file,
        resources=compute_resource,
        is_delete_operator_pod=True,
        get_logs=True)

    t1 = PythonOperator(
        task_id='python_function_sleep_lol',
        python_callable=lol,
        dag=dag,
    )
