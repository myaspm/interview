from datetime import datetime, timedelta
from uuid import uuid1
from json import dumps, loads

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.dates import days_ago
from airflow.contrib.sensors.python_sensor import PythonSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['sysadmin@spp42.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

data_dag = DAG(
    'seffaflik_data',
    default_args=default_args,
    description='Fetch Data From Seffaflik API',
    schedule_interval=timedelta(days=1),
)


def get_jobs(ds, **kwargs):
    api_hook = HttpHook(http_conn_id="seffaflik_api", method="GET", )
    resp = api_hook.run('get_jobs')
    return loads(resp.content)


def post_orgs(ds, **kwargs):
    task_instance = kwargs['task_instance']
    jobs = task_instance.xcom_pull(task_ids='get_jobs')
    if "kudup" in jobs["jobs"]:
        api_hook = HttpHook(http_conn_id="seffaflik_api", method="POST")
        data_dict = {"job_id": str(uuid1()),
                     "params": {"jobs": ["org"]}}
        resp = api_hook.run(endpoint="run",
                            data=dumps(data_dict).encode("utf-8"))
        return loads(resp.content)["query_id"]
    else:
        pass


def post_kgup(ds, **kwargs):
    task_instance = kwargs['task_instance']
    jobs = task_instance.xcom_pull(task_ids='get_jobs')
    if "kgup" in jobs["jobs"]:
        api_hook = HttpHook(http_conn_id="seffaflik_api", method="POST")
        data_dict = {"job_id": str(uuid1()),
                     "params": {"jobs": ["kgup"]}}
        resp = api_hook.run(endpoint="run",
                            data=dumps(data_dict).encode("utf-8"))
        return loads(resp.content)["query_id"]
    else:
        pass


def post_kudup(ds, **kwargs):
    task_instance = kwargs['task_instance']
    jobs = task_instance.xcom_pull(task_ids='get_jobs')
    if "kudup" in jobs["jobs"]:
        api_hook = HttpHook(http_conn_id="seffaflik_api", method="POST")
        data_dict = {"job_id": str(uuid1()),
                     "params": {"jobs": ["kudup"]}}
        resp = api_hook.run(endpoint="run",
                            data=dumps(data_dict).encode("utf-8"))
        return loads(resp.content)["query_id"]
    else:
        pass


def post_tr(ds, **kwargs):
    task_instance = kwargs['task_instance']
    jobs = task_instance.xcom_pull(task_ids='get_jobs')
    if "tr" in jobs["jobs"]:
        api_hook = HttpHook(http_conn_id="seffaflik_api", method="POST")
        data_dict = {"job_id": str(uuid1()),
                     "params": {"jobs": ["tr"]}}
        resp = api_hook.run(endpoint="run",
                            data=dumps(data_dict).encode("utf-8"))
        return loads(resp.content)["query_id"]
    else:
        pass


def post_generation(ds, **kwargs):
    task_instance = kwargs['task_instance']
    jobs = task_instance.xcom_pull(task_ids='get_jobs')
    if "tr" in jobs["jobs"]:
        api_hook = HttpHook(http_conn_id="seffaflik_api", method="POST")
        data_dict = {"job_id": str(uuid1()),
                     "params": {"jobs": ["generation"]}}
        resp = api_hook.run(endpoint="run",
                            data=dumps(data_dict).encode("utf-8"))
        return loads(resp.content)["query_id"]
    else:
        pass


def success(job_type, **kwargs):
    if job_type == "all":
        return True
    else:
        print("Trying {}".format(job_type))
        task_instance = kwargs['task_instance']
        query_id = task_instance.xcom_pull(task_ids=job_type)
        api_hook = HttpHook(http_conn_id="seffaflik_api", method="GET")
        resp = api_hook.run('query_job/{}'.format(query_id))
        result = loads(resp.content)
        if result["status"] == "SUCCESS":
            return True
        return False


get_jobs_task = PythonOperator(task_id='get_jobs',
                               provide_context=True,
                               python_callable=get_jobs,
                               dag=data_dag)

get_org_task = PythonOperator(task_id='get_orgs',
                              provide_context=True,
                              python_callable=post_orgs,
                              dag=data_dag)

get_kgup_task = PythonOperator(task_id='get_kgup',
                               provide_context=True,
                               python_callable=post_kgup,
                               dag=data_dag)

get_kudup_task = PythonOperator(task_id='get_kudup',
                                provide_context=True,
                                python_callable=post_kudup,
                                dag=data_dag)

get_tr_task = PythonOperator(task_id='get_tr',
                             provide_context=True,
                             python_callable=post_tr,
                             dag=data_dag)

get_generation_task = PythonOperator(task_id='get_generation',
                                     provide_context=True,
                                     python_callable=post_generation,
                                     dag=data_dag)

success_sensor_kgup = PythonSensor(
    start_date=datetime.now() - timedelta(days=1),
    task_id="success_kgup",
    python_callable=success,
    provide_context=True,
    poke_interval=60,
    op_args=["get_kgup"]
)

success_sensor_kudup = PythonSensor(
    start_date=datetime.now() - timedelta(days=1),
    task_id="success_kudup",
    python_callable=success,
    provide_context=True,
    poke_interval=60,
    op_args=["get_kudup"]
)

success_sensor_tr = PythonSensor(
    start_date=datetime.now() - timedelta(days=1),
    task_id="success_tr",
    python_callable=success,
    provide_context=True,
    poke_interval=60,
    op_args=["get_tr"]
)

success_sensor_org = PythonSensor(
    start_date=datetime.now() - timedelta(days=1),
    task_id="success_org",
    python_callable=success,
    provide_context=True,
    poke_interval=60,
    op_args=["get_orgs"]
)

success_sensor_gen = PythonSensor(
    start_date=datetime.now() - timedelta(days=1),
    task_id="success_gen",
    python_callable=success,
    provide_context=True,
    poke_interval=60,
    op_args=["get_generation"]
)

success_sensor_all = PythonSensor(
    start_date=datetime.now() - timedelta(days=1),
    task_id="success_all",
    op_args=["all"],
    python_callable=success,
    poke_interval=30,
    retries=20
)

get_jobs_task.set_downstream([get_org_task,
                              get_generation_task,
                              get_kgup_task,
                              get_kudup_task,
                              get_tr_task])

success_sensor_kgup.set_upstream(get_kgup_task)
success_sensor_org.set_upstream(get_org_task)
success_sensor_gen.set_upstream(get_generation_task)
success_sensor_kudup.set_upstream(get_kudup_task)
success_sensor_tr.set_upstream(get_tr_task)

success_sensor_all.set_upstream([success_sensor_org,
                                 success_sensor_tr,
                                 success_sensor_kgup,
                                 success_sensor_gen,
                                 success_sensor_kudup])
