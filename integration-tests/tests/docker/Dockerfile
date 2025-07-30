FROM ghcr.io/netcracker/qubership-docker-integration-tests:0.2.2

ENV ROBOT_OUTPUT=${ROBOT_HOME}/output \
    DISTR_DIR=/tmp/deps \
    SERVICE_CHECKER_SCRIPT=${ROBOT_HOME}/airflow_pods_checker.py \
    SERVICE_CHECKER_SCRIPT_TIMEOUT=500

ENV STATUS_CUSTOM_RESOURCE_GROUP=apps \
    STATUS_CUSTOM_RESOURCE_VERSION=v1 \
    STATUS_CUSTOM_RESOURCE_PLURAL=deployments \
    STATUS_CUSTOM_RESOURCE_NAME=airflow-integration-tests

RUN mkdir -p ${ROBOT_HOME} \
    && mkdir -p ${ROBOT_OUTPUT}

COPY docker/requirements.txt ${ROBOT_HOME}/requirements.txt
COPY robot ${ROBOT_HOME}
COPY docker/airflow_pods_checker.py ${ROBOT_HOME}/airflow_pods_checker.py

RUN set -x \
    && pip3 install --no-cache-dir -r ${ROBOT_HOME}/requirements.txt \
    && rm -rf /var/cache/apk/*

USER 1000:0

EXPOSE 8080
VOLUME ["${ROBOT_OUTPUT}"]
