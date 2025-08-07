# flake8: noqa

# This DAG can be used to copy data between two Hadoop clusters(HDFS),
# Hive postgres database and airflow postgres database.

import json
import os
import logging
import time
import datetime

import requests

from datetime import timedelta

from airflow.models import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

from airflow.providers.standard.operators.empty import EmptyOperator

copy_pg_hive_db_command = """#!/bin/bash
set -e

# export DRHIVEPGHOSTTOBACKUPPASSWORD="hive_pg_password_main"
# export DRHIVEPGHOSTTOBACKUPUSER="hive_pg_user_main"
# export DRHIVEPGHOSTTOBACKUPDB="hive_pg_db_main"
# export DRHIVEPGHOSTTOBACKUPPORT=5432
# export DRHIVEPGHOSTTOBACKUP="pg-patroni-ro.postgres.svc"
# export DRHIVEPGHOSTRESTOREPASSWORD="hive_pg_password_dr"
# export DRHIVEPGHOSTRESTOREUSER="hive_pg_user_dr"
# export DRHIVEPGHOSTRESTOREDB="hive_pg_db_dr"
# export DRHIVEPGHOSTRESTOREPORT=5432
# export DRHIVEPGHOSTRESTORE="pg-patroni-direct.postgres.svc"

export PGPASSWORD="${DRHIVEPGHOSTTOBACKUPPASSWORD}";
pg_dump -F c -h "${DRHIVEPGHOSTTOBACKUP}" -U "${DRHIVEPGHOSTTOBACKUPUSER}" -p "${DRHIVEPGHOSTTOBACKUPPORT}" "${DRHIVEPGHOSTTOBACKUPDB}" > backupeddb.tar
echo "backup created"

export PGPASSWORD="${DRHIVEPGHOSTRESTOREPASSWORD}";
# dropdb "${DRHIVEPGHOSTRESTOREDB}" -U "${DRHIVEPGHOSTRESTOREUSER}" -h "${DRHIVEPGHOSTRESTORE}" -p "${DRHIVEPGHOSTRESTOREPORT}"
# echo "dr db dropped"

# createdb "${DRHIVEPGHOSTRESTOREDB}" -U "${DRHIVEPGHOSTRESTOREUSER}" -h "${DRHIVEPGHOSTRESTORE}" -p "${DRHIVEPGHOSTRESTOREPORT}"
# echo "dr db recreated"

set +e
pg_restore -h "${DRHIVEPGHOSTRESTORE}" -p "${DRHIVEPGHOSTRESTOREPORT}" -d "${DRHIVEPGHOSTRESTOREDB}" -U "${DRHIVEPGHOSTRESTOREUSER}" backupeddb.tar
set -e

echo "dr db restored"
rm backupeddb.tar"""

copy_pg_hive_db_command_with_dropping = """#!/bin/bash
set -e

# export DRHIVEPGHOSTTOBACKUPPASSWORD="hive_pg_password_main"
# export DRHIVEPGHOSTTOBACKUPUSER="hive_pg_user_main"
# export DRHIVEPGHOSTTOBACKUPDB="hive_pg_db_main"
# export DRHIVEPGHOSTTOBACKUPPORT=5432
# export DRHIVEPGHOSTTOBACKUP="pg-patroni-ro.postgres.svc"
# export DRHIVEPGHOSTRESTOREPASSWORD="hive_pg_password_dr"
# export DRHIVEPGHOSTRESTOREUSER="hive_pg_user_dr"
# export DRHIVEPGHOSTRESTOREDB="hive_pg_db_dr"
# export DRHIVEPGHOSTRESTOREPORT=5432
# export DRHIVEPGHOSTRESTORE="pg-patroni-direct.postgres.svc"

export PGPASSWORD="${DRHIVEPGHOSTTOBACKUPPASSWORD}";
pg_dump -F c -h "${DRHIVEPGHOSTTOBACKUP}" -U "${DRHIVEPGHOSTTOBACKUPUSER}" -p "${DRHIVEPGHOSTTOBACKUPPORT}" "${DRHIVEPGHOSTTOBACKUPDB}" > backupeddb.tar
echo "backup created"

export PGPASSWORD="${DRHIVEPGHOSTRESTOREPASSWORD}";
# dropdb "${DRHIVEPGHOSTRESTOREDB}" -U "${DRHIVEPGHOSTRESTOREUSER}" -h "${DRHIVEPGHOSTRESTORE}" -p "${DRHIVEPGHOSTRESTOREPORT}"
psql -U "${DRHIVEPGHOSTRESTOREUSER}" -d postgres -h "${DRHIVEPGHOSTRESTORE}" -p "${DRHIVEPGHOSTRESTOREPORT}" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '${DRHIVEPGHOSTRESTOREDB}';" -c "DROP DATABASE ${DRHIVEPGHOSTRESTOREDB};"
echo "dr db dropped"

createdb "${DRHIVEPGHOSTRESTOREDB}" -U "${DRHIVEPGHOSTRESTOREUSER}" -h "${DRHIVEPGHOSTRESTORE}" -p "${DRHIVEPGHOSTRESTOREPORT}"
echo "dr db recreated"

set +e
pg_restore -h "${DRHIVEPGHOSTRESTORE}" -p "${DRHIVEPGHOSTRESTOREPORT}" -d "${DRHIVEPGHOSTRESTOREDB}" -U "${DRHIVEPGHOSTRESTOREUSER}" backupeddb.tar
set -e

echo "dr db restored"
rm backupeddb.tar"""

copy_pg_airflow_db_command_with_dropping = """set -e

# export DRAIRFLOWPGHOSTTOBACKUPPASSWORD="airflow_pg_password_main"
# export DRAIRFLOWPGHOSTTOBACKUPUSER="airflow_pg_user_main"
# export DRAIRFLOWPGHOSTTOBACKUPDB="airflow_pg_db_main"
# export DRAIRFLOWPGHOSTTOBACKUPPORT=31111
# export DRAIRFLOWPGHOSTTOBACKUP="airflow.pg.addr.main.com"
# export DRAIRFLOWPGHOSTRESTOREPASSWORD="airflow_pg_password_dr"
# export DRAIRFLOWPGHOSTRESTOREUSER="airflow_pg_user_dr"
# export DRAIRFLOWPGHOSTRESTOREDB="airflow_pg_db_dr"
# export DRAIRFLOWPGHOSTRESTOREPORT=31111
# export DRAIRFLOWPGHOSTRESTORE="airflow.pg.addr.dr.com"
# export DRAIRFLOWPGHOSTRESTOREOWNER="airflow_owner_dr"


PGPASSWORD="${DRAIRFLOWPGHOSTRESTOREPASSWORD}" psql -U "${DRAIRFLOWPGHOSTRESTOREUSER}" -d postgres -h "${DRAIRFLOWPGHOSTRESTORE}" -p "${DRAIRFLOWPGHOSTRESTOREPORT}" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '${DRAIRFLOWPGHOSTRESTOREDB}';" -c "DROP DATABASE ${DRAIRFLOWPGHOSTRESTOREDB};"

echo "dr db dropped"

PGPASSWORD="${DRAIRFLOWPGHOSTRESTOREPASSWORD}" createdb "${DRAIRFLOWPGHOSTRESTOREDB}" -U "${DRAIRFLOWPGHOSTRESTOREUSER}" -h "${DRAIRFLOWPGHOSTRESTORE}" -p "${DRAIRFLOWPGHOSTRESTOREPORT}"
echo "dr db recreated"



PGPASSWORD="${DRAIRFLOWPGHOSTRESTOREPASSWORD}" psql -U "${DRAIRFLOWPGHOSTRESTOREUSER}" -d postgres -h "${DRAIRFLOWPGHOSTRESTORE}" -p "${DRAIRFLOWPGHOSTRESTOREPORT}" -c "GRANT ALL PRIVILEGES ON DATABASE ${DRAIRFLOWPGHOSTRESTOREDB} TO ${DRAIRFLOWPGHOSTRESTOREOWNER};"

echo "permissions granted"

PGPASSWORD="${DRAIRFLOWPGHOSTRESTOREPASSWORD}" psql -U "${DRAIRFLOWPGHOSTRESTOREUSER}" -d postgres -h "${DRAIRFLOWPGHOSTRESTORE}" -p "${DRAIRFLOWPGHOSTRESTOREPORT}" -c "ALTER DATABASE ${DRAIRFLOWPGHOSTRESTOREDB} OWNER TO ${DRAIRFLOWPGHOSTRESTOREOWNER};"

echo "owner changed"

set +e
PGPASSWORD="${DRAIRFLOWPGHOSTTOBACKUPPASSWORD}" pg_dump -F c -h "${DRAIRFLOWPGHOSTTOBACKUP}" -U "${DRAIRFLOWPGHOSTTOBACKUPUSER}" -p "${DRAIRFLOWPGHOSTTOBACKUPPORT}" "${DRAIRFLOWPGHOSTTOBACKUPDB}" | PGPASSWORD="${DRAIRFLOWPGHOSTTOBACKUPPASSWORD}" pg_restore -h "${DRAIRFLOWPGHOSTRESTORE}" -p "${DRAIRFLOWPGHOSTRESTOREPORT}" -d "${DRAIRFLOWPGHOSTRESTOREDB}" -U "${DRAIRFLOWPGHOSTRESTOREUSER}" --single-transaction

"""

args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2025, 1, 1),
}

dr_delta_hours = os.getenv("DR_DAG_TIMEDELTA", 2)

dag = DAG(
    dag_id="DR_SYNC_DAG_V2",
    default_args=args,
    # schedule_interval="*/1 * * * *",
    schedule=timedelta(hours=float(dr_delta_hours)),  # "@hourly"
    max_active_runs=1,
    tags=["Platform"],
    catchup=False,
)

WEBHDFS_CONN_ID_MAIN = "webhdfs_default"
WEBHDFS_CONN_ID_DR = "webhdfs_dr"

folders = os.getenv("DR_FOLDER_LIST", "/backup")
airflow_user = os.getenv("AIRFLOW_SSH_USER_FOR_KINIT")
folder_list = folders.split(",")
keytab_file_path = os.getenv("KEYTAB_FILE_PATH_FOR_DR_SYNC", "airflow.keytab")
use_security = os.getenv("DO_KINIT_ON_THE_NODE", "True")
snapshot_prefix = "snapshot_"

AMBARI_DR_URL = os.getenv("AMBARI_DR_URL", "http://your.ambari.address.com:8080")
AMBARI_DR_USER = os.getenv("AMBARI_DR_USER", "ambari_user")
AMBARI_DR_PASSWORD = os.getenv("AMBARI_DR_PASSWORD", "ambari_password")
AMBARI_DR_CLUSTER_NAME = os.getenv("AMBARI_DR_CLUSTER_NAME", "hadoop_cluster")


def check_request_completed(request_number):
    print("waiting for request to complete")
    request_completed = False
    timeout_10 = 180
    while not request_completed:
        headers = {
            "X-Requested-By": "ambari",
        }

        response = requests.get(
            f"{AMBARI_DR_URL}/api/v1/clusters/{AMBARI_DR_CLUSTER_NAME}/requests/{request_number}",
            headers=headers,
            verify=False,
            auth=(AMBARI_DR_USER, AMBARI_DR_PASSWORD),
        )
        if response.status_code != 200:
            logging.error("WRONG RESPONSE STATUS CODE")
            raise Exception("WRONG RESPONSE STATUS CODE RECEIVED")
        else:
            logging.info("correct code")
        content_string = response.content.decode("utf-8")
        content_json = json.loads(content_string)
        if content_json["Requests"]["request_status"] != "COMPLETED":
            logging.info(
                f"wrong request status: {content_json['Requests']['request_status']}"
            )
            logging.info(f"waiting, time left:{timeout_10 * 10}")
            timeout_10 = timeout_10 - 1
            if timeout_10 < 0:
                raise Exception("Timeout reached")
            time.sleep(10)
        else:
            logging.info("Request completed")
            request_completed = True


def check_response(response):
    if response.status_code != 202:
        logging.error("WRONG RESPONSE STATUS CODE")
        raise Exception("WRONG RESPONSE STATUS CODE RECEIVED")
    else:
        logging.info("correct code")
    content_string = response.content.decode("utf-8")
    content_json = json.loads(content_string)
    return content_json["Requests"]["id"]


def stop_hive_service():
    headers = {
        "X-Requested-By": "ambari",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = '{"RequestInfo": {"context" :"Stop Hive service"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}'

    response = requests.put(
        f"{AMBARI_DR_URL}/api/v1/clusters/{AMBARI_DR_CLUSTER_NAME}/services/HIVE",
        headers=headers,
        data=data,
        verify=False,
        auth=(AMBARI_DR_USER, AMBARI_DR_PASSWORD),
    )
    request_id = check_response(response)
    check_request_completed(request_id)


def start_hive_service():
    headers = {
        "X-Requested-By": "ambari",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = '{"RequestInfo": {"context" :"Start Hive service"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'

    response = requests.put(
        f"{AMBARI_DR_URL}/api/v1/clusters/{AMBARI_DR_CLUSTER_NAME}/services/HIVE",
        headers=headers,
        data=data,
        verify=False,
        auth=(AMBARI_DR_USER, AMBARI_DR_PASSWORD),
    )
    request_id = check_response(response)
    check_request_completed(request_id)


def get_dr_folder_name(folder_name):
    # ToDo allow specifying different folder names for main/DR
    return folder_name


def get_latest_snapshot_number(web_hdfs_hook, folder_name):
    if not web_hdfs_hook.check_for_path(f"{folder_name}/.snapshot"):
        return -1
    web_hdfs_client = web_hdfs_hook.get_conn()
    snapshots = web_hdfs_client.list(f"{folder_name}/.snapshot")
    number_list = []
    import re

    for snapshot in snapshots:
        search_result = re.search(r"^" + snapshot_prefix + r"(\d+)\Z", snapshot)
        if search_result:
            number_list.append(int(search_result.group(1)))
    if len(number_list) == 0:
        return -1
    return max(number_list)


def parse_hdfs_hostname(client):
    webhdfs_address = client.url
    from urllib.parse import urlparse

    return urlparse(webhdfs_address).hostname


def get_ssh_host(ds, **kwargs):
    hdfs_dr_hook = WebHDFSHook(webhdfs_conn_id=WEBHDFS_CONN_ID_DR)
    dr_client = hdfs_dr_hook.get_conn()
    dr_hdfs_addr = parse_hdfs_hostname(dr_client)
    kwargs["ti"].xcom_push(key="dr_ssh_addr", value=dr_hdfs_addr)


def check_folder_state(ds, **kwargs):
    folder_to_verify = kwargs["folder"]
    dr_folder_to_verify = get_dr_folder_name(folder_to_verify)
    hdfs_main_hook = WebHDFSHook(webhdfs_conn_id=WEBHDFS_CONN_ID_MAIN)
    logging.info("checking that folder exists on main site ...")
    if not hdfs_main_hook.check_for_path(folder_to_verify):
        raise ValueError("Folder not found on main site!")
    main_client = hdfs_main_hook.get_conn()

    hdfs_dr_hook = WebHDFSHook(webhdfs_conn_id=WEBHDFS_CONN_ID_DR)
    logging.info("checking that folder exists on DR site ...")
    if not hdfs_dr_hook.check_for_path(dr_folder_to_verify):
        logging.info("folder not found on DR site ...")
        raise ValueError("Folder not found on DR site!")
    dr_client = hdfs_dr_hook.get_conn()

    main_hdfs_addr = parse_hdfs_hostname(main_client)
    dr_hdfs_addr = parse_hdfs_hostname(dr_client)

    main_latest_snapshot_number = get_latest_snapshot_number(
        hdfs_main_hook, folder_to_verify
    )
    dr_latest_snapshot_number = get_latest_snapshot_number(
        hdfs_dr_hook, dr_folder_to_verify
    )

    # todo find valid SSH host and put it to xcom to use it as remote_host for ssh operator

    logging.info("checking main folder snapshots state ...")
    if main_latest_snapshot_number == -1:  #
        logging.info(
            "Snapshots not found for main site, checking that DR folder is empty ..."
        )
        if (
            len(dr_client.list(dr_folder_to_verify)) > 0
            or dr_latest_snapshot_number != -1
        ):
            # ToDo maybe add logic for cleaning DR folder
            raise ValueError(
                "Snapshots not found on main site, but DR folder is not empty!"
            )
        main_client.create_snapshot(folder_to_verify, f"{snapshot_prefix}1")
        kwargs["ti"].xcom_push(
            key="source_folder",
            value=f"hdfs://{main_hdfs_addr}{folder_to_verify}/.snapshot/{snapshot_prefix}1",
        )
        kwargs["ti"].xcom_push(
            key="target_folder", value=f"hdfs://{dr_hdfs_addr}{dr_folder_to_verify}"
        )
        return f"initialize_folder_{folder_to_verify.replace('/', '_')}"

    logging.info("checking DR folder snapshots state ...")
    if dr_latest_snapshot_number == -1:
        if len(dr_client.list(dr_folder_to_verify)) > 0:
            raise ValueError(
                "Snapshots found on main site, not on DR and DR folder is not empty!"
            )
        logging.warning(
            f"Found {snapshot_prefix}{main_latest_snapshot_number} on main site, trying to continue from it"
        )
        main_client.delete_snapshot(
            folder_to_verify, f"{snapshot_prefix}{main_latest_snapshot_number}"
        )
        main_client.create_snapshot(
            folder_to_verify, f"{snapshot_prefix}{main_latest_snapshot_number}"
        )
        kwargs["ti"].xcom_push(
            key="source_folder",
            value=f"hdfs://{main_hdfs_addr}{folder_to_verify}/.snapshot/{snapshot_prefix}{main_latest_snapshot_number}",
        )
        kwargs["ti"].xcom_push(
            key="target_folder", value=f"hdfs://{dr_hdfs_addr}{dr_folder_to_verify}"
        )
        return f"initialize_folder_{folder_to_verify.replace('/', '_')}"

    if main_latest_snapshot_number != dr_latest_snapshot_number:
        raise ValueError(
            "Latest snapshot numbers are different, manual resolution required!"
        )
    logging.info(f"latest snapshot number found: {main_latest_snapshot_number}")

    main_client.create_snapshot(
        folder_to_verify, f"{snapshot_prefix}{main_latest_snapshot_number + 1}"
    )
    kwargs["ti"].xcom_push(
        key="source_snapshot", value=f"{snapshot_prefix}{main_latest_snapshot_number}"
    )
    kwargs["ti"].xcom_push(
        key="target_snapshot",
        value=f"{snapshot_prefix}{main_latest_snapshot_number + 1}",
    )
    kwargs["ti"].xcom_push(
        key="source_folder", value=f"hdfs://{main_hdfs_addr}{folder_to_verify}"
    )
    kwargs["ti"].xcom_push(
        key="target_folder", value=f"hdfs://{dr_hdfs_addr}{dr_folder_to_verify}"
    )
    return f"update_folder_{folder_to_verify.replace('/', '_')}"


def validate_distcp_successful(ds, **kwargs):
    folder_to_verify = kwargs["folder"]
    dr_folder_to_verify = get_dr_folder_name(folder_to_verify)
    hdfs_dr_hook = WebHDFSHook(webhdfs_conn_id=WEBHDFS_CONN_ID_DR)
    dr_client = hdfs_dr_hook.get_conn()
    hdfs_main_hook = WebHDFSHook(webhdfs_conn_id=WEBHDFS_CONN_ID_MAIN)
    main_client = hdfs_main_hook.get_conn()
    latest_snapshot_number = get_latest_snapshot_number(
        hdfs_main_hook, folder_to_verify
    )
    dr_client.create_snapshot(
        dr_folder_to_verify, f"{snapshot_prefix}{latest_snapshot_number}"
    )
    main_list = main_client.list(
        f"{folder_to_verify}/.snapshot/{snapshot_prefix}{latest_snapshot_number}"
    )
    dr_list = dr_client.list(
        f"{dr_folder_to_verify}/.snapshot/{snapshot_prefix}{latest_snapshot_number}"
    )
    logging.info("comparing lists after the copy ...")
    if dr_list != main_list:
        raise ValueError("Lists different after the copy!")
    main_content = main_client.content(
        f"{folder_to_verify}/.snapshot/{snapshot_prefix}{latest_snapshot_number}"
    )
    dr_content = dr_client.content(
        f"{dr_folder_to_verify}/.snapshot/{snapshot_prefix}{latest_snapshot_number}"
    )
    logging.info("comparing contents after the copy ...")
    if dr_content["directoryCount"] != main_content["directoryCount"]:
        logging.warning("directoryCount is different after the copy!")
    if dr_content["fileCount"] != main_content["fileCount"]:
        logging.warning("fileCount is different after the copy!")
    if dr_content["length"] != main_content["length"]:
        logging.warning("length is different after the copy!")
    logging.info("Content looks similar ...")
    if hdfs_main_hook.check_for_path(
        f"{folder_to_verify}/.snapshot/{snapshot_prefix}{latest_snapshot_number - 1}"
    ):
        main_client.delete_snapshot(
            folder_to_verify, f"{snapshot_prefix}{latest_snapshot_number - 1}"
        )
    if hdfs_dr_hook.check_for_path(
        f"{dr_folder_to_verify}/.snapshot/{snapshot_prefix}{latest_snapshot_number - 1}"
    ):
        dr_client.delete_snapshot(
            dr_folder_to_verify, f"{snapshot_prefix}{latest_snapshot_number - 1}"
        )


pre_task = PythonOperator(
    task_id="find_correct_ssh_host",
    python_callable=get_ssh_host,
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

task0 = (
    SSHOperator(
        ssh_conn_id="ssh_dr",
        remote_host='{{ task_instance.xcom_pull(task_ids=["find_correct_ssh_host"], key="dr_ssh_addr")[0] }}',
        task_id="do_kinit",
        command=f"kinit -kt  {keytab_file_path} {airflow_user}",
        cmd_timeout=604800,
        dag=dag,
    )
    if use_security != "False"
    else EmptyOperator(task_id="do_kinit", dag=dag)
)

stop_hive = PythonOperator(
    task_id="stop_hive",
    python_callable=stop_hive_service,
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

copy_pg_hive_database = BashOperator(
    task_id="copy_pg_hive_database",
    bash_command=copy_pg_hive_db_command_with_dropping,
    dag=dag,
)

copy_pg_airflow_database = BashOperator(
    task_id="copy_pg_airflow_database",
    bash_command=copy_pg_airflow_db_command_with_dropping,
    dag=dag,
)

start_hive = PythonOperator(
    task_id="start_hive",
    python_callable=start_hive_service,
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

pre_task >> stop_hive
stop_hive >> task0

for folder in folder_list:
    task2 = BranchPythonOperator(
        task_id=f"verify_{folder.replace('/', '_')}_state",
        python_callable=check_folder_state,
        op_kwargs={"folder": folder},
        dag=dag,
    )

    t3 = SSHOperator(
        ssh_conn_id="ssh_dr",
        task_id=f"initialize_folder_{folder.replace('/', '_')}",
        remote_host='{{ task_instance.xcom_pull(task_ids=["find_correct_ssh_host"], key="dr_ssh_addr")[0] }}',
        command=f"hadoop distcp -pugpax -blocksperchunk 1000 -update "
        f'{{{{ task_instance.xcom_pull(task_ids=["verify_{folder.replace("/", "_")}_state"], key="source_folder")[0] }}}} '
        f'{{{{ task_instance.xcom_pull(task_ids=["verify_{folder.replace("/", "_")}_state"], key="target_folder")[0] }}}} ',
        cmd_timeout=604800,
        do_xcom_push=True,
        dag=dag,
    )

    t4 = SSHOperator(
        ssh_conn_id="ssh_dr",
        task_id=f"update_folder_{folder.replace('/', '_')}",
        remote_host='{{ task_instance.xcom_pull(task_ids=["find_correct_ssh_host"], key="dr_ssh_addr")[0] }}',
        command=f"hadoop distcp -pugpax -blocksperchunk 1000 -diff "
        f'{{{{ task_instance.xcom_pull(task_ids=["verify_{folder.replace("/", "_")}_state"], key="source_snapshot")[0] }}}} '
        f'{{{{ task_instance.xcom_pull(task_ids=["verify_{folder.replace("/", "_")}_state"], key="target_snapshot")[0] }}}} '
        f"-update "
        f'{{{{ task_instance.xcom_pull(task_ids=["verify_{folder.replace("/", "_")}_state"], key="source_folder")[0] }}}} '
        f'{{{{ task_instance.xcom_pull(task_ids=["verify_{folder.replace("/", "_")}_state"], key="target_folder")[0] }}}} ',
        cmd_timeout=604800,
        do_xcom_push=True,
        dag=dag,
    )

    task5 = PythonOperator(
        task_id=f"post_update_{folder.replace('/', '_')}",
        python_callable=validate_distcp_successful,
        op_kwargs={"folder": folder},
        trigger_rule="none_failed_min_one_success",
        dag=dag,
    )

    task0 >> task2
    task2 >> t3
    task2 >> t4
    t3 >> task5
    t4 >> task5
    task5 >> start_hive

copy_pg_airflow_database
stop_hive >> copy_pg_hive_database
copy_pg_hive_database >> start_hive
