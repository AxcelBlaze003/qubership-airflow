# DR DAG v1 for history

import os

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python import BranchPythonOperator
import base64

FOLDERS_NOT_INITIALIZED_SUBSTRING = "FOLDERS WERE NOT INITIALIZED"
FOLDERS_INITIALIZED_SUBSTRING = "FOLDERS WERE INITIALIZED, LATEST SNAPSHOT NUMBER:"

args = {
    "owner": "airflow",
    "start_date": days_ago(2),
}

dag = DAG(
    dag_id="DR_SYNC_DAG",
    default_args=args,
    # schedule_interval="*/1 * * * *",
    schedule_interval="@hourly",
    tags=["Platform"],
    catchup=False,
)

# ToDo Upload keytab logic goes here

nodes_main = os.getenv("NODES_MAIN")
nodes_dr = os.getenv("NODES_DR")
folders = os.getenv("DR_FOLDER_LIST")
airflow_user = os.getenv("AIRFLOW_USER")
folder_list = folders.split(",")


def verify_folder_state(**kwargs):
    folder_to_verify = kwargs["folder"]
    base64enc_init_output = kwargs["ti"].xcom_pull(
        task_ids=f'check_state_for_folder_{folder_to_verify.replace("/", "_")}',
        key="return_value",
    )
    init_output = base64.b64decode(base64enc_init_output).decode()
    print("return value: ", init_output.split("\n"))
    if FOLDERS_NOT_INITIALIZED_SUBSTRING in init_output:
        return f'initialize_folder_{folder_to_verify.replace("/", "_")}'
    elif FOLDERS_INITIALIZED_SUBSTRING in init_output:
        latest_snapshot_number = (
            init_output.split(FOLDERS_INITIALIZED_SUBSTRING)[1]
            .split("\n")[0]
            .replace(" ", "")
        )
        print("latest snapshot number: ", latest_snapshot_number)
        kwargs["ti"].xcom_push(
            key="latest_snapshot_number", value=latest_snapshot_number
        )
        return f'update_folder_{folder_to_verify.replace("/", "_")}'
    else:
        raise ValueError("Something went wrong during folder verification")


# ToDo add task to find valid SSH host and use it as remote_host for ssh operator


task0 = SSHOperator(
    ssh_conn_id="ssh_dr",
    task_id="do_kinit",
    command=f"kinit -kt  airflow.keytab {airflow_user}",
    dag=dag,
)

for folder in folder_list:
    task1 = SSHOperator(
        ssh_conn_id="ssh_dr",
        task_id=f'check_state_for_folder_{folder.replace("/", "_")}',
        command=f"./airflow_user_dr_copy.sh --nodes_dr {nodes_dr} --nodes_main {nodes_main} "
        f"--foldername {folder}  --action get_latest",
        do_xcom_push=True,
        dag=dag,
    )

    task2 = BranchPythonOperator(
        task_id=f'verify_{folder.replace("/", "_")}_state',
        python_callable=verify_folder_state,
        op_kwargs={"folder": folder, "key2": "value2"},
        dag=dag,
    )

    t3 = SSHOperator(
        ssh_conn_id="ssh_dr",
        task_id=f'initialize_folder_{folder.replace("/", "_")}',
        command=f"./airflow_user_dr_copy.sh --nodes_dr {nodes_dr} --nodes_main {nodes_main} "
        f"--foldername {folder}  --action init",
        do_xcom_push=True,
        dag=dag,
    )
    t4 = SSHOperator(
        ssh_conn_id="ssh_dr",
        task_id=f'update_folder_{folder.replace("/", "_")}',
        command=f"./airflow_user_dr_copy.sh --nodes_dr {nodes_dr} --nodes_main {nodes_main} "
        f"--foldername {folder}  --action update --snapshot_number "
        f'{{{{ task_instance.xcom_pull(task_ids=["verify_{folder.replace("/", "_")}_state"], '
        f'key="latest_snapshot_number")[0] }}}}',
        do_xcom_push=True,
        dag=dag,
    )

    task0 >> task1
    task1 >> task2
    task2 >> t3
    task2 >> t4
