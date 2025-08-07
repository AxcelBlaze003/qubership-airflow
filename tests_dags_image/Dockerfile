ARG base_img=ghcr.io/netcracker/qubership-airflow:main

FROM $base_img

# Arguments
ARG AIRFLOW_USER_HOME=/opt/airflow
#
#ENV KAFKA_CONF_DIR=/etc/hadoop/conf

USER root

COPY tests_dags_image/dags/ ${AIRFLOW_USER_HOME}/dags/
COPY tests_dags_image/get_hadoop_conf.sh ${AIRFLOW_USER_HOME}/get_hadoop_conf.sh
COPY tests_dags_image/add_s3_policy.py /bin/
# https://github.com/xmlsec/python-xmlsec/issues/336
RUN apt-get --no-install-recommends install -y default-jre sasl2-bin libsasl2-2 libsasl2-modules libsasl2-modules-gssapi-mit pkg-config libxml2-dev libxmlsec1-dev libxmlsec1-openssl \
    && apt-get clean

RUN chmod 0777 ${AIRFLOW_USER_HOME}/get_hadoop_conf.sh \
    && mkdir -p /etc/hadoop/conf \
    && chown airflow /etc/hadoop/conf

#Copy requirements folder
COPY tests_dags_image/requirements.txt ${AIRFLOW_USER_HOME}/test_requirements.txt

USER airflow
# Install apache-airflow-backport-providers-cncf-kubernetes python package
RUN python3 -m pip install -r ${AIRFLOW_USER_HOME}/test_requirements.txt

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV HADOOP_USER_NAME=hdfs
ENV HADOOP_CONF_DIR=/etc/hadoop/conf

RUN curl -k  https://repo1.maven.org/maven2/com/sun/jersey/jersey-bundle/1.17/jersey-bundle-1.17.jar -o jersey-bundle-1.17.jar \
    && mv jersey-bundle-1.17.jar $SPARK_HOME/jars

# COPY docker/idpapiintegrationpackage /idpapiintegrationpackage
# RUN chmod 777 -R /idpapiintegrationpackagecd /idpapiintegrationpackage \
#     && python3 -m pip install . \
#     && rm -r /idpapiintegrationpackage/*
