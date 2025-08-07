import os
import requests
import json
import logging
import base64
import ssl

from airflow.secrets.base_secrets import BaseSecretsBackend

logging_level = os.getenv("DBAAS_INTEGRATION_LOG_LEVEL", logging.INFO)

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] [%(filename)s] [thread=%(threadName)s] %(message)s",
    level=logging_level,
)

positive_values = ("true", "True", "yes", "Yes", True)
negative_values = ("false", "False", "no", "No", False)

dbaas_host = os.getenv("DBAAS_HOST")
dbaas_user = os.getenv("DBAAS_USER")
dbaas_password = os.getenv("DBAAS_PASSWORD")
dbaas_conn_namespace_from_config = os.getenv(
    "DBAAS_CONN_NAMESPACE_FROM_CONFIG", "false"
)
maas_conn_namespace_from_config = os.getenv("MAAS_CONN_NAMESPACE_FROM_CONFIG", "true")

maas_host = os.getenv("MAAS_HOST")
maas_user = os.getenv("MAAS_USER")
maas_password = os.getenv("MAAS_PASSWORD")
dbaas_api_verify = os.getenv("DBAAS_API_VERIFY", True)
dbaas_ssl_verification_main = os.getenv("DBAAS_SSL_VERIFICATION_MAIN", "DISABLED")


class DBAASSecretsBackend(BaseSecretsBackend):
    def __init__(self, **kwargs):
        super().__init__()
        self.qs_secrets_backend_properties = kwargs
        self.api_verify = (
            False if dbaas_api_verify in negative_values else dbaas_api_verify
        )
        with open(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        ) as namespace_file:
            self.namespace = namespace_file.read()

    def get_conn_value(self, conn_id: str):
        if self.qs_secrets_backend_properties == {}:
            return None
        if f"{conn_id}_dbaas" in self.qs_secrets_backend_properties:
            logging.info(
                f"found {conn_id} in backend properties, will be received from DBAAS."
            )
            dbaas_data = self.qs_secrets_backend_properties[f"{conn_id}_dbaas"]
            connection_data = self.get_conn_from_dbaas(dbaas_data)
            if dbaas_data["type"] == "postgresql":
                return self.parse_pg_connection(connection_data)
        if f"{conn_id}_maas" in self.qs_secrets_backend_properties:
            logging.info(
                f"found {conn_id} in backend properties, will be received from MAAS."
            )
            maas_type = self.qs_secrets_backend_properties[f"{conn_id}_maas"][
                "maas_type"
            ]
            logging.debug(f"maas type: {maas_type}")
            maas_data = self.qs_secrets_backend_properties[f"{conn_id}_maas"][
                "maas_request_data"
            ]
            logging.debug(f"maas data: {maas_data}")
            maas_patch = self.qs_secrets_backend_properties[f"{conn_id}_maas"][
                "connection_properties"
            ]
            logging.debug(f"extra patch: {maas_patch}")
            if maas_type == "kafka":
                connection_data = self.get_kafka_conn_from_maas(maas_data)
                logging.debug(f"connection data: {connection_data}")
                return self.parse_kafka_connection(connection_data, maas_patch)

        return None

    def get_kafka_conn_from_maas(self, maas_data):
        maas_headers_namespace = self.namespace
        if maas_conn_namespace_from_config in positive_values:
            maas_headers_namespace = maas_data["classifier"]["namespace"]
        headers = {
            "Content-Type": "application/json",
            "X-Origin-Namespace": maas_headers_namespace,
        }
        auth = (maas_user, maas_password)
        maas_kafka_api_address = f"{maas_host}/api/v1/kafka/topic"
        response = requests.post(
            maas_kafka_api_address,
            headers=headers,
            data=json.dumps(maas_data),
            auth=auth,
            verify=self.api_verify,
        )
        if response.status_code == 201 or response.status_code == 200:
            return json.loads(response.content)
        else:
            logging.error(
                "[error_code=AIRFLOW-8304] Could not get MAAS Kafka database."
                " Response code: %s",
                response.status_code,
            )
        return None

    @staticmethod
    def parse_kafka_connection(connection_data, additional_extra_data):
        if "SASL_PLAINTEXT" in connection_data["addresses"]:
            security_protocol = "SASL_PLAINTEXT"
            addresses = ",".join(connection_data["addresses"]["SASL_PLAINTEXT"])
        elif "PLAINTEXT" in connection_data["addresses"]:
            security_protocol = "PLAINTEXT"
            addresses = ",".join(connection_data["addresses"]["PLAINTEXT"])
        elif "SSL" in connection_data["addresses"]:
            security_protocol = "SSL"
            addresses = ",".join(connection_data["addresses"]["SSL"])
        elif "SASL_SSL" in connection_data["addresses"]:
            security_protocol = "SASL_SSL"
            addresses = ",".join(connection_data["addresses"]["SASL_SSL"])
        else:
            logging.error(
                "[error_code=AIRFLOW-8305] Unsupported authentication mechanism for kafka addresses"
            )
            return None
        logging.debug(f"security protocol is {security_protocol}")
        kafka_credentials = connection_data["credential"]
        logging.debug("received kafka credentials ...")
        if "client" not in kafka_credentials:
            logging.error(
                "[error_code=AIRFLOW-8306] Client credentials not found for Kafka!"
            )
            return None
        sasl_mechanism = None
        kafka_password_with_prefix = None
        kafka_username = None
        logging.debug("parsing kafka credentials...")
        for creds in kafka_credentials["client"]:
            kafka_password_with_prefix = creds["password"]
            if creds["type"] == "SCRAM" and kafka_password_with_prefix.startswith(
                "plain:"
            ):
                sasl_mechanism = "SCRAM-SHA-512"
                kafka_username = creds["username"]
                break
            elif creds["type"] == "plain" and kafka_password_with_prefix.startswith(
                "plain:"
            ):
                sasl_mechanism = "PLAIN"
                kafka_username = creds["username"]
                break
            else:
                logging.warning(
                    "found creds with not supported sasl mechanism or password format, looking further"
                )
        if sasl_mechanism is None or kafka_password_with_prefix is None:
            logging.error(
                "[error_code=AIRFLOW-8307] Not supported sasl mechanism or password format!"
            )
            return None
        logging.debug(
            f"sasl mechanism is{sasl_mechanism}, username is {kafka_username}"
        )
        kafka_password = kafka_password_with_prefix.replace("plain:", "", 1)

        connection_extra_initial = {
            "bootstrap.servers": addresses,
            "sasl.username": kafka_username,
            "sasl.password": kafka_password,
            "sasl.mechanism": sasl_mechanism,
            "security.protocol": security_protocol,
        }
        if (
            "caCert" in connection_data
            and "ssl.ca.location" not in additional_extra_data
        ):
            logging.debug(
                "converting kafka caCert received from DBAAS to required format"
            )
            connection_extra_initial["ssl.ca.pem"] = ssl.DER_cert_to_PEM_cert(
                base64.b64decode(connection_data["caCert"])
            )
        # patching connection with data from parameters
        connection_extra = connection_extra_initial | additional_extra_data
        from airflow.models.connection import Connection

        kafka_conn = Connection(conn_type="kafka", extra=json.dumps(connection_extra))
        return kafka_conn.get_uri()

    def get_conn_from_dbaas(self, dbaas_data):
        headers = {"Content-Type": "application/json"}
        auth = (dbaas_user, dbaas_password)
        if dbaas_conn_namespace_from_config in positive_values:
            logging.debug("Using namespace from config for dbaas request")
            namespace = dbaas_data["classifier"]["namespace"]
        else:
            logging.debug("Using airflow namespace for dbaas request")
            namespace = self.namespace
        db_type = dbaas_data["type"]
        dbaas_by_classifier_data = {"classifier": dbaas_data["classifier"]}
        if "originService" in dbaas_data:
            dbaas_by_classifier_data.update(
                {"originService": dbaas_data["originService"]}
            )
        response = requests.post(
            f"{dbaas_host}/api/v3/dbaas/{self.namespace}/databases/get-by-classifier/{db_type}",
            headers=headers,
            data=json.dumps(dbaas_by_classifier_data),
            auth=auth,
            verify=self.api_verify,
        )
        if response.status_code == 200:
            return json.loads(response.content)["connectionProperties"]
        logging.debug(
            f"Could not get airflow DAG database by classifier {dbaas_by_classifier_data},"
            f" trying to get by database ..."
        )
        response = requests.put(
            f"{dbaas_host}/api/v3/dbaas/{namespace}/databases",
            headers=headers,
            data=json.dumps(dbaas_data),
            auth=auth,
            verify=self.api_verify,
        )
        if response.status_code == 201 or response.status_code == 200:
            return json.loads(response.content)["connectionProperties"]
        else:
            logging.error(
                "[error_code=AIRFLOW-8302] Could not get DBAAS airflow connection database."
                " Response code: %s",
                response.status_code,
            )

    @staticmethod
    def parse_pg_connection(connection_data):
        if "dbName" in connection_data:
            dbname = connection_data["dbName"]
        elif "name" in connection_data:
            dbname = connection_data["name"]
        else:
            logging.error(
                "[error_code=AIRFLOW-8303] DBAAS returned connection string for PG does"
                "not contain database name"
            )
            dbname = None
        pg_no_ssl_address = (
            f"{connection_data['username']}:{connection_data['password']}@{connection_data['host']}:"
            f"{connection_data['port']}/{dbname}"
        )
        pg_ssl_mode = "sslmode=disable"
        if "tls" in connection_data and connection_data["tls"] in positive_values:
            if dbaas_ssl_verification_main == "ENABLED":
                pg_ssl_mode = "sslmode=verify-ca"
            elif dbaas_ssl_verification_main.startswith("CERT_PATH:"):
                pg_ssl_mode = f'sslmode=verify-ca&sslrootcert={dbaas_ssl_verification_main.removeprefix("CERT_PATH:")}'
            elif dbaas_ssl_verification_main == "DISABLED":
                pg_ssl_mode = "sslmode=require"
            else:
                logging.error(
                    "[error_code=AIRFLOW-8308] Incorrect parameter for SSL certificate verification"
                )
                pg_ssl_mode = "sslmode=require"
        logging.debug(
            f"Parsed pg connection: {connection_data['username']}:*****@{connection_data['host']}:"
            f"{connection_data['port']}/{dbname}?{pg_ssl_mode}"
        )
        pg_full_address = f"{pg_no_ssl_address}?{pg_ssl_mode}"
        # TODO change get_conn_value to get_connection() and return connection!
        connection_string = f"postgresql://{pg_full_address}"
        return connection_string

    def get_pg_database(self):
        logging.debug(
            f"DBAAS_properties type:{type(self.qs_secrets_backend_properties)}"
        )
        logging.debug(f"DBAAS properties value:{self.qs_secrets_backend_properties}")
        dbaas_pg_db_owner = os.getenv("DBAAS_PG_DB_OWNER", "airflow")
        dbaas_pg_backup_disabled = os.getenv("DBAAS_PG_BACKUP_DISABLED", "true")
        dbaas_pg_microservice_name = os.getenv("DBAAS_PG_MICROSERVICE_NAME", "airflow")
        headers = {"Content-Type": "application/json"}
        auth = (dbaas_user, dbaas_password)
        data = {
            "originService": dbaas_pg_microservice_name,
            "classifier": {
                "namespace": self.namespace,
                "scope": "service",
                "microserviceName": dbaas_pg_microservice_name,
                "isServiceDb": "true",
            },
        }
        response = requests.post(
            f"{dbaas_host}/api/v3/dbaas/{self.namespace}/databases/get-by-classifier/postgresql",
            headers=headers,
            data=json.dumps(data),
            auth=auth,
            verify=self.api_verify,
        )
        if response.status_code == 200:
            return json.loads(response.content)["connectionProperties"]
        logging.warning(
            "Could not get airflow main PG database by classifier, trying to get by database ..."
        )
        data.update(
            {
                "backupDisabled": dbaas_pg_backup_disabled,
                "type": "postgresql",
                "dbOwner": dbaas_pg_db_owner,
            }
        )
        dbaas_pg_db_name_prefix = os.getenv("DBAAS_PG_DB_NAME_PREFIX")
        if dbaas_pg_db_name_prefix is not None:
            data["namePrefix"] = dbaas_pg_db_name_prefix
        response = requests.put(
            f"{dbaas_host}/api/v3/dbaas/{self.namespace}/databases",
            headers=headers,
            data=json.dumps(data),
            auth=auth,
            verify=self.api_verify,
        )
        if response.status_code == 201 or response.status_code == 200:
            return json.loads(response.content)["connectionProperties"]
        else:
            logging.error(
                "[error_code=AIRFLOW-8301] Could not get or create DBAAS PG database. Response code: %s",
                response.status_code,
            )

    def get_redis_database(self):
        dbaas_redis_db_owner = os.getenv("DBAAS_REDIS_DB_OWNER", "airflow")
        dbaas_redis_backup_disabled = os.getenv("DBAAS_REDIS_BACKUP_DISABLED", "true")
        dbaas_redis_microservice_name = os.getenv(
            "DBAAS_REDIS_MICROSERVICE_NAME", "airflow"
        )
        headers = {"Content-Type": "application/json"}
        auth = (dbaas_user, dbaas_password)
        data = {
            "originService": dbaas_redis_microservice_name,
            "classifier": {
                "namespace": self.namespace,
                "scope": "service",
                "microserviceName": dbaas_redis_microservice_name,
                "isServiceDb": "true",
            },
        }
        response = requests.post(
            f"{dbaas_host}/api/v3/dbaas/{self.namespace}/databases/get-by-classifier/redis",
            headers=headers,
            data=json.dumps(data),
            auth=auth,
            verify=self.api_verify,
        )
        if response.status_code == 200:
            return json.loads(response.content)["connectionProperties"]
        logging.warning(
            "Could not get airflow main redis database by classifier, trying to get by database ..."
        )
        data.update(
            {
                "backupDisabled": dbaas_redis_backup_disabled,
                "type": "redis",
                "dbOwner": dbaas_redis_db_owner,
            }
        )
        dbaas_redis_db_name_prefix = os.getenv("DBAAS_REDIS_DB_NAME_PREFIX")
        if dbaas_redis_db_name_prefix is not None:
            data["namePrefix"] = dbaas_redis_db_name_prefix

        response = requests.put(
            f"{dbaas_host}/api/v3/dbaas/{self.namespace}/databases",
            headers=headers,
            data=json.dumps(data),
            auth=auth,
            verify=self.api_verify,
        )
        if response.status_code == 201 or response.status_code == 200:
            return json.loads(response.content)["connectionProperties"]
        else:
            logging.error(
                "[error_code=AIRFLOW-8300] Could not get or create DBAAS Redis database. Response code: %s",
                response.status_code,
            )

    def get_pg_connection(self):
        pg_connection = self.get_pg_database()
        connection_string = self.parse_pg_connection(pg_connection)
        return connection_string

    def get_redis_connection(self):
        redis_db_name = os.getenv("DBAAS_REDIS_DB_NAME_PREFIX", "1")
        redis_connection = self.get_redis_database()
        if "dbName" in redis_connection:
            redis_dbname = redis_connection["dbName"]
        elif "name" in redis_connection:
            redis_dbname = redis_connection["name"]
        else:
            logging.warning(
                "DBAAS returned connection string for redis does not contain database name"
            )
            redis_dbname = redis_db_name
        redis_prefix = "redis://"
        redis_ssl_mode = ""
        if "tls" in redis_connection and redis_connection["tls"] in positive_values:
            redis_prefix = "rediss://"
            if (
                dbaas_ssl_verification_main == "ENABLED"
                or dbaas_ssl_verification_main == "CERT_PATH:system"
            ):
                redis_ssl_mode = "?ssl_cert_reqs=CERT_REQUIRED"
            elif dbaas_ssl_verification_main.startswith("CERT_PATH:"):
                redis_ssl_mode = (
                    f"?ssl_cert_reqs=CERT_REQUIRED&ssl_ca_certs="
                    f'{dbaas_ssl_verification_main.removeprefix("CERT_PATH:")}'
                )
            elif dbaas_ssl_verification_main == "DISABLED":
                redis_ssl_mode = "?ssl_cert_reqs=CERT_NONE"
            else:
                logging.error(
                    "[error_code=AIRFLOW-8308] Incorrect parameter for SSL certificate verification"
                )
                redis_ssl_mode = "?ssl_cert_reqs=CERT_NONE"
        logging.debug(
            f"redis address: {redis_prefix}default:*****@{redis_connection['host']}:"
            f"{redis_connection['port']}/{redis_dbname}{redis_ssl_mode}"
        )
        redis_full_address = (
            f"{redis_prefix}default:{redis_connection['password']}@{redis_connection['host']}:"
            f"{redis_connection['port']}/{redis_dbname}{redis_ssl_mode}"
        )
        return redis_full_address

    def get_variable(self, key: str):
        return None

    def get_config(self, key: str):
        """
        Return value for Airflow Config Key.

        :param key: Config Key
        :return: Config Value
        """
        # ToDo reformat key to conn_id
        if key == "airfow_celery_redis_main_conn":
            return self.get_redis_connection()
        if key == "airfow_pg_main_conn":
            return self.get_pg_connection()
        return None
