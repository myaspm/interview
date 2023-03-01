from datetime import datetime, timedelta
from os import remove

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.utils.dates import days_ago

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

ftp_dag = DAG(
    'scada_ftp',
    default_args=default_args,
    description='Fetch Scada From FTP and Write to MongoDB',
    schedule_interval=timedelta(days=1),
)


def retrieve_file_list(ds, **kwargs):
    start_date = datetime.today() - timedelta(days=1)
    start_date_str = start_date.strftime("%Y%m%d")
    ftp_hook = FTPHook(ftp_conn_id="scada_ftp")
    return list(ftp_hook.list_directory("files/" + start_date_str))


def insert_to_mongo(ds, **kwargs):
    ftp_hook = FTPHook(ftp_conn_id="scada_ftp")
    ftp_conn = ftp_hook.get_conn()

    mongo_hook = MongoHook(conn_id="mongod")
    scada = dict()
    scada["meter_id"] = "9a9dab20-6da2-11ea-b501-02420aff0ac2"
    start_date = datetime.today() - timedelta(days=1)
    scada["scada_date"] = start_date.replace(hour=0,
                                             minute=0,
                                             second=0,
                                             microsecond=0
                                             )
    start_date_str = start_date.strftime("%Y%m%d")
    ftp_conn.cwd("files/" + start_date_str)
    task_instance = kwargs['task_instance']
    ten_min_file_list = task_instance.xcom_pull(task_ids='retrieve_tasks')
    for i in range(0, 24):
        hour_file_list = sorted([
            file_name for file_name in ten_min_file_list if "{} {}".format(
                start_date_str, "{:02d}".format(i)
            ) in file_name
        ])

        turbine_total = 0
        hour_file_count = len(hour_file_list)
        if hour_file_count != 0:
            for hour_file in hour_file_list:
                with open(hour_file, "wb") as ftp_read_file:
                    ftp_conn.retrbinary('RETR '
                                        + hour_file, ftp_read_file.write)
                with open(hour_file, "r",
                          errors="ignore") as ten_min_file_read:
                    ftp_lines = ten_min_file_read.readlines()[3:18]
                    for line in ftp_lines:
                        turbine_total += float(line.split(";")[1])
                remove(hour_file)
            end_result = turbine_total / hour_file_count / 1000
            if end_result < 0:
                end_result = 0.0
            scada["h{:02d}".format(i)] = end_result
        else:
            scada["h{:02d}".format(i)] = None
    print(scada)
    mongo_hook.update_one("scada",
                          {"scada_date": scada["scada_date"]},
                          {"$set": scada},
                          "prod_kahin",
                          upsert=True)


def empty_folder_callback(ds, **kwargs):
    scada = dict()
    scada["meter"] = "9a9dab20-6da2-11ea-b501-02420aff0ac2"
    start_date = datetime.today() - timedelta(days=1)
    scada["scada_date"] = start_date.replace(hour=0,
                                             minute=0,
                                             second=0,
                                             microsecond=0)
    mongo_hook = MongoHook(conn_id="mongod")
    for hour in range(0, 24):
        scada["h{:02d}".format(hour)] = ""
    mongo_hook.insert_one("scada", scada, "prod_kahin")


retrive_files_task = PythonOperator(task_id='retrieve_tasks',
                                    provide_context=True,
                                    python_callable=retrieve_file_list,
                                    on_failure_callback=empty_folder_callback,
                                    dag=ftp_dag)

insert_mongo_task = PythonOperator(task_id='insert_to_mongo',
                                   provide_context=True,
                                   python_callable=insert_to_mongo,
                                   dag=ftp_dag)

retrive_files_task.set_downstream(insert_mongo_task)