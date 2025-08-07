import os
import requests
import json
import logging

from kubernetes import client as client, config as k8s_config
from kubernetes.client import ApiException

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] [%(filename)s] [thread=%(threadName)s] %(message)s",
    level=logging.INFO,
)

dbaas_host = os.getenv("DBAAS_HOST")
dbaas_user = os.getenv("DBAAS_USER")
dbaas_password = os.getenv("DBAAS_PASSWORD")
airflow_executor = os.getenv("AIRFLOW_EXECUTOR", "CeleryExecutor")

negative_values = ("false", "False", "no", "No", False)
dbaas_api_verify = os.getenv("DBAAS_API_VERIFY", True)
api_verify = False if dbaas_api_verify in negative_values else dbaas_api_verify

k8s_config.load_incluster_config()
k8s_client = client.ApiClient()
v1_apps_api = client.CoreV1Api(k8s_client)


def get_namespace():
    with open(
        "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    ) as namespace_file:
        return namespace_file.read()


def delete_secret_ignore_not_found(name, grace_period_seconds=20):
    try:
        v1_apps_api.delete_namespaced_secret(
            name=name,
            namespace=get_namespace(),
            grace_period_seconds=grace_period_seconds,
        )
    except ApiException as exceptondelete:
        # ignore missing objects
        if exceptondelete.status == 404:
            logging.warning("Cannot delete object because it cannot be found")
        else:
            raise


def get_redis_database():
    dbaas_redis_db_owner = os.getenv("DBAAS_REDIS_DB_OWNER", "airflow")
    dbaas_redis_backup_disabled = os.getenv("DBAAS_REDIS_BACKUP_DISABLED", "true")
    dbaas_redis_microservice_name = os.getenv(
        "DBAAS_REDIS_MICROSERVICE_NAME", "airflow"
    )
    headers = {"Content-Type": "application/json"}
    data = {
        "backupDisabled": dbaas_redis_backup_disabled,
        "type": "redis",
        "dbOwner": dbaas_redis_db_owner,
        "originService": dbaas_redis_microservice_name,
        "classifier": {
            "namespace": get_namespace(),
            "scope": "service",
            "microserviceName": dbaas_redis_microservice_name,
            "isServiceDb": "true",
        },
    }
    dbaas_redis_db_name_prefix = os.getenv("DBAAS_REDIS_DB_NAME_PREFIX")
    if dbaas_redis_db_name_prefix is not None:
        data["namePrefix"] = dbaas_redis_db_name_prefix

    auth = (dbaas_user, dbaas_password)

    response = requests.put(
        f"{dbaas_host}/api/v3/dbaas/{get_namespace()}/databases",
        headers=headers,
        data=json.dumps(data),
        auth=auth,
        verify=api_verify,
    )
    if response.status_code == 201 or response.status_code == 200:
        # ToDo do not print sensitive info
        # print(response.content)
        return json.loads(response.content)["connectionProperties"]
    else:
        logging.error(
            "[error_code=AIRFLOW-8300] Could not get DBAAS Redis database. Response code: %s",
            response.status_code,
        )


def get_pg_database():
    dbaas_pg_db_owner = os.getenv("DBAAS_PG_DB_OWNER", "airflow")
    dbaas_pg_backup_disabled = os.getenv("DBAAS_PG_BACKUP_DISABLED", "true")
    dbaas_pg_microservice_name = os.getenv("DBAAS_PG_MICROSERVICE_NAME", "airflow")
    headers = {"Content-Type": "application/json"}
    data = {
        "backupDisabled": dbaas_pg_backup_disabled,
        "type": "postgresql",
        "dbOwner": dbaas_pg_db_owner,
        "originService": dbaas_pg_microservice_name,
        "classifier": {
            "namespace": get_namespace(),
            "scope": "service",
            "microserviceName": dbaas_pg_microservice_name,
            "isServiceDb": "true",
        },
    }
    dbaas_pg_db_name_prefix = os.getenv("DBAAS_PG_DB_NAME_PREFIX")
    if dbaas_pg_db_name_prefix is not None:
        data["namePrefix"] = dbaas_pg_db_name_prefix

    auth = (dbaas_user, dbaas_password)

    response = requests.put(
        f"{dbaas_host}/api/v3/dbaas/{get_namespace()}/databases",
        headers=headers,
        data=json.dumps(data),
        auth=auth,
        verify=api_verify,
    )
    if response.status_code == 201 or response.status_code == 200:
        # ToDo do not print sensitive info
        # print(response.content)
        return json.loads(response.content)["connectionProperties"]
    else:
        logging.error(
            "[error_code=AIRFLOW-8301] Could not get DBAAS PG database. Response code: %s",
            response.status_code,
        )


def create_pg_secret():
    try:
        get_pg_database()
        # delete secrets in case of upgrade from old version
        delete_secret_ignore_not_found("metadata-secret")
        # second secret for  resultBackendSecretName
        # connection_string = f'db+postgresql://{pg_full_address}'
        # delete_secret_ignore_not_found("result-backend-secret")
        # secret = V1Secret(api_version='v1', string_data={'connection': connection_string},
        #                   kind='Secret',
        #                   metadata=V1ObjectMeta(name="result-backend-secret", namespace=namespace))
        # v1_apps_api.create_namespaced_secret(namespace=namespace, body=secret)
    except Exception:
        logging.exception(
            "[error_code=AIRFLOW-1930] Error occurred while creating PG secret."
        )


def create_redis_secret():
    try:
        get_redis_database()
        delete_secret_ignore_not_found("broker-url-secret")
    except Exception:
        logging.exception(
            "[error_code=AIRFLOW-1931] Error occurred while creating redis secret."
        )


create_pg_secret()
if airflow_executor != "KubernetesExecutor":
    create_redis_secret()
