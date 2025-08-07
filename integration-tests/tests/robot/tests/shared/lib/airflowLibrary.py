from PlatformLibrary import PlatformLibrary
import requests
import json
import logging
import base64
import re

pl_lib = PlatformLibrary(managed_by_operator="true")


def get_pg_connection_properties(
    namespace, dbaas_secret="dbaas-connection-params-main", pg_secret="airflow-metadata"
):
    secrets = pl_lib.get_secrets(namespace)
    for secret in secrets.items:
        if dbaas_secret == secret.metadata.name:
            dbaas_host = base64.b64decode(secret.data.get("DBAAS_HOST")).decode()
            dbaas_user = base64.b64decode(secret.data.get("DBAAS_USER")).decode()
            dbaas_password = base64.b64decode(
                secret.data.get("DBAAS_PASSWORD")
            ).decode()
            dbaas_pg_db_owner = base64.b64decode(
                secret.data.get("DBAAS_PG_DB_OWNER")
            ).decode()
            dbaas_pg_backup_disabled = base64.b64decode(
                secret.data.get("DBAAS_PG_BACKUP_DISABLED")
            ).decode()
            dbaas_pg_microservice_name = base64.b64decode(
                secret.data.get("DBAAS_PG_MICROSERVICE_NAME")
            ).decode()
            headers = {"Content-Type": "application/json"}
            data = {
                "backupDisabled": dbaas_pg_backup_disabled,
                "type": "postgresql",
                "dbOwner": dbaas_pg_db_owner,
                "originService": dbaas_pg_microservice_name,
                "classifier": {
                    "namespace": namespace,
                    "scope": "service",
                    "microserviceName": dbaas_pg_microservice_name,
                    "isServiceDb": "true",
                },
            }
            auth = (dbaas_user, dbaas_password)
            response = requests.put(
                f"{dbaas_host}/api/v3/dbaas/{namespace}/databases",
                headers=headers,
                data=json.dumps(data),
                auth=auth,
            )
            if response.status_code == 201 or response.status_code == 200:
                connectionProperties = json.loads(response.content)[
                    "connectionProperties"
                ]
                return (
                    connectionProperties["host"],
                    connectionProperties["username"],
                    connectionProperties["password"],
                    connectionProperties["name"],
                )
            else:
                logging.error(
                    "[error_code=AIRFLOW-8301] Could not get DBAAS PG database. Response code: %s",
                    response.status_code,
                )
        elif pg_secret == secret.metadata.name:
            pg_connection = base64.b64decode(secret.data.get("connection")).decode()
            pattern = re.compile(
                r"([^:]+)://([^:]+):([^@]+)@([^:/]+):(\d+)/([^?]+)\?(\w+=[^&]+&?)*"
            )
            match = pattern.match(pg_connection)
            if match:
                conn = match.groups()
                pg_host = conn[3]
                pg_user = conn[1]
                pg_password = conn[2]
                pg_database = conn[5]
            else:
                logging.error(
                    "PG connection from secret does not match the expected pattern."
                )
            return pg_host, pg_user, pg_password, pg_database


def identify_dags_manager(name, namespace):
    deployment = pl_lib.get_deployment_entity(name, namespace)
    for container in deployment.spec.template.spec.containers:
        if container.name == "git-sync":
            for mount in container.volume_mounts:
                if mount.name == "rclone-config":
                    return "rclone"
            return "gitsync"
    return None
