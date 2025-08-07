#!/usr/bin/env bash
# shellcheck disable=all

# This file can be used to get hadoop config from Ambari or Cloudera Manager

set -x

# Usage message
print_usage() {
  echo "To automatically load the Hadoop configuration, you must set the following variables:
          In case of AMBARI Manager:
          AMBARI_USER AMBARI_PASSWORD AMBARI_URL AMBARI_PORT CLUSTER_NAME HADOOP_DISTRIBUTION=HDP
          In case of Cloudera Manager:
          CM_HOST CM_PORT CM_USER CM_PASSWORD CM_SERVICE_ID HADOOP_DISTRIBUTION=CDH
          This can be done in the {.values.airflow.config} section for cloud deploy.
          If docker-compose is used, the specified variables should be declared in the
          datahub-etl.env file
          If you see this message, then one or more variables are undefined and
          loading the Hadoop configuration will be skipped."
}
# List of variables to get hadoop conf from AMBARI
AMBARIVariablesArray=(
  AMBARI_USER \
  AMBARI_PASSWORD \
  AMBARI_URL \
  AMBARI_PORT \
  CLUSTER_NAME \
  )

ClouderaVariablesArray=(
  CM_HOST \
  CM_PORT \
  CM_USER \
  CM_PASSWORD \
  CM_SERVICE_ID \
)


# Check that all variables are defined
if [ ${HADOOP_DISTRIBUTION} == "" ]; then
  print_usage
  exit 0
elif [ ${HADOOP_DISTRIBUTION} == "HDP" ]; then
  for item in ${AMBARIVariablesArray[@]}; do
    if [ -z ${!item} ]; then
      print_usage
      exit 0
    fi
  done
elif [ ${HADOOP_DISTRIBUTION} == "CDH" ]; then
  for item in ${ClouderaVariablesArray[@]}; do
    if [ -z ${!item} ]; then
      print_usage
      exit 0
    fi
  done
else
  print_usage
  exit 0
fi

echo "Start hadoop conf creation"
export TMP_HCC=~/tmp_hadoop_conf_creation
rm -rf $TMP_HCC
mkdir $TMP_HCC

if [ ${HADOOP_DISTRIBUTION} == "HDP" ]; then
  AMBARI_HOST=`echo $AMBARI_URL | awk -F[/:] '{print $4}'`
  curlop=$(curl --insecure -s --write-out %{http_code} --user $AMBARI_USER:$AMBARI_PASSWORD https://$AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$CLUSTER_NAME/ -o /dev/null)
  if [ $curlop -eq "000" ]; then
    PROTOCOL="http"
  else
    echo "Importing Root CA Cert"
    PROTOCOL="https"
    echo "yes" | /usr/bin/keytool -import -trustcacerts -cacerts -file /opt/airflow/certs/rootca -alias root-ca -storepass changeit
  fi

  curl --insecure --create-dirs --user $AMBARI_USER:$AMBARI_PASSWORD -H "X-Requested-By: AMBARI" -X GET $PROTOCOL://$AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$CLUSTER_NAME/components?format=client_config_tar -o $TMP_HCC/hadoop-conf.tar
  mkdir $TMP_HCC/{unpacked,conf}
  tar -xvf $TMP_HCC/hadoop-conf.tar -C $TMP_HCC/unpacked/  --warning=no-timestamp
  for folder in $TMP_HCC/unpacked/* ; do
    if [ -d "$folder" ]; then
      cp $folder/* $TMP_HCC/conf/
    fi
  done
  cp -r $TMP_HCC/conf/ /etc/hadoop/

elif [ ${HADOOP_DISTRIBUTION} == "CDH" ]; then

  curl -D - -o - -X POST http://$CM_HOST:$CM_PORT/j_spring_security_check --data \
   "j_username=$CM_USER&j_password=$CM_PASSWORD&returnUrl=&submit=" -c $TMP_HCC/cookie
  curl -b $TMP_HCC/cookie http://$CM_HOST:$CM_PORT/cmf/services/$CM_SERVICE_ID/client-config > $TMP_HCC/hadoop.zip
  curl -b $TMP_HCC/cookie http://$CM_HOST:$CM_PORT/cmf/logout

  unzip -d $TMP_HCC/ $TMP_HCC/hadoop.zip
  cp $TMP_HCC/hive-conf/* /etc/hadoop/conf/

fi

rm -rf $TMP_HCC
echo "Hadoop configuration updated"
