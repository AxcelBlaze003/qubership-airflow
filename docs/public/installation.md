This section provides information about the Airflow installation using [slightly modified airflow helm chart](/chart/helm/airflow) and [custom qubership platform airflow docker image](/docker/Dockerfile). The helm chart and image changes are described in [readme.md](/README.MD).

- [General Information](#general-information)
  - [Image Changes from Community Version](#image-changes-from-community-version)
  - [Deployment Schema](#deployment-schema)
- [Prerequisites](#prerequisites)
  - [Common](#common)
    - [Restricted Deploy User with Site Manager](#restricted-deploy-user-with-site-manager)
  - [OpenShift](#openshift)
  - [AWS](#aws)
- [Best Practices and Recommendations](#best-practices-and-recommendations)
  - [HWE](#hwe)
    - [Small](#small)  
    - [Medium](#medium)
    - [Large](#large)
- [Parameters](#parameters)
  - [Helm Parameter Configuration Changes from Community Version](#helm-parameter-configuration-changes-from-community-version)
  - [Airflow Site Manager and DR Deployment](#airflow-site-manager-and-dr-deployment)
  - [Custom Preinstall Job](#custom-preinstall-job)
      - [Precreating PostgreSQL Database](#precreating-postgresql-database)
      - [DBaaS Integration](#dbaas-integration)
  - [DBaaS Integration for Airflow Connections](#dbaas-integration-for-airflow-connections)
      - [MaaS Integration for Airflow Connections](#maas-integration-for-airflow-connections)
  - [Creating a Secret for Custom Keycloak Client Registration](#creating-a-secret-for-custom-keycloak-client-registration)
  - [Specifying External Broker](#specifying-external-broker)
  - [Specifying SSL Connections to Redis and Postgres](#specifying-ssl-connections-to-redis-and-postgres)
      - [Redis Connection Configuration](#redis-connection-configuration)
      - [Postgres Connection Configuration](#postgres-connection-configuration)
      - [Getting CA Certificate From Cert-Manager](#getting-ca-certificate-from-cert-manager)
      - [Using SSL with Connections Received from DBaaS](#using-ssl-with-connections-received-from-dbaas)
  - [Airflow Logging Config Classes](#airflow-logging-config-classes)
      - [Default Logging Config Class](#default-logging-config-class)
      - [Enabling Custom Logging Config Class](#enabling-custom-logging-config-class)
      - [Audit Logs](#audit-logs)
  - [Cleaning Airflow Logs](#cleaning-airflow-logs)
  - [Using Custom Pre-start Command for Pods](#using-custom-pre-start-command-for-pods)
  - [Creating Custom Secrets](#creating-custom-secrets)
  - [Using Git Sync](#using-git-sync)
      - [Using Community GitSync Approach](#using-community-gitsync-approach)
      - [Using GitSync With Custom Mount Path](#using-gitsync-with-custom-mount-path)
  - [Using Rclone to Sync DAGs From Remote Storage](#using-rclone-to-sync-dags-from-remote-storage)
      - [Using TLS for Rclone Endpoint](#using-tls-for-rclone-endpoint)
      - [Remote Cloud Storage](#remote-cloud-storage)
      - [Using Downloadable ZIP Archive with DAGs](#using-downloadable-zip-archive-with-dags)
  - [Kerberos Support and HostAliases for Workers](#kerberos-support-and-hostaliases-for-workers)
  - [LDAP Support for Web UI](#ldap-support-for-web-ui)
  - [Keycloak Web UI Integration](#keycloak-web-ui-integration)
    - [Keycloak With TLS](#keycloak-with-tls)
  - [Enabling HTTPS for Airflow Ingresses](#enabling-https-for-airflow-ingresses)
      - [Using Cert-manager to Get Certificate for Ingress](#using-cert-manager-to-get-certificate-for-ingress)
  - [Prometheus Monitoring and Alerts](#prometheus-monitoring-and-alerts)
      - [Plugin Prometheus Monitoring](#plugin-prometheus-monitoring)
      - [StatsD Prometheus Exporter Monitoring](#statsd-prometheus-exporter-monitoring)
      - [Grafana Dashboard](#grafana-dashboard)
      - [Prometheus Alerts](#prometheus-alerts)
  - [Status Provisioner Job](#status-provisioner-job)
  - [Integration Tests](#integration-tests)
- [Installation](#installation)
  - [On-Prem](#on-prem)
    - [HA Scheme](#ha-scheme)
    - [DR Scheme](#dr-scheme)
    - [Non-HA Scheme](#non-ha-scheme)
  - [AWS](#aws)
  - [Manual Helm Installation](#manual-helm-installation)

# General Information

For more information about Airflow Service, refer to the _Airflow Repository_ at [https://github.com/apache/airflow](https://github.com/apache/airflow).
For more information about Airflow Helm parameters and configuration, refer to the _Official Airflow Helm Documentation_ at [https://airflow.apache.org/docs/helm-chart/stable/index.html](https://airflow.apache.org/docs/helm-chart/stable/index.html).

## Image Changes from Community Version

The base Airflow image in addition to Airflow (airflow:slim-3.0.2-python3.11) contains the following libraries:

* comerr-dev
* unzip
* build-essential
* manpages-dev
* libkrb5-dev
* libsasl2-dev
* python3-dev
* libldap2-dev
* libssl-dev
* libpq-dev

Also, the image contains the following Python libraries/Airflow extras:

* apache-airflow[celery,kerberos,ldap,statsd,rabbitmq,postgres,kubernetes]==3.0.2
* airflow-exporter

The image contains Python script for the PG database creation.

Also, the image has a package for Qubership DBaaS/MaaS integration.

## Deployment Schema

For more information about the deployment schema, refer to the [Overview](/docs/public/architecture.md).

**Note**: In the Quberhip tested configuration, the Airflow executor is set to Celery executor or Kubernetes executor. By default, the Celery executor is used. To use a Kubernetes executor, set the executor parameter to KubernetesExecutor: `executor: "KubernetesExecutor"`. Also, `rbac.create` parameter must be set to `true` to allow airflow to launch kubernetes executor workers.

**Note**: To upgrade from celeryExecutor to kubernetesExecutor and vice versa, helm uninstallation is needed.

**Note**: According to the Airflow helm chart production guide at [https://airflow.apache.org/docs/helm-chart/stable/production-guide.html#pgbouncer](https://airflow.apache.org/docs/helm-chart/stable/production-guide.html#pgbouncer), it is recommended to use Airflow helm chart with PG Bouncer. However, Qubership used and tested configuration of Airflow does not support the deployment of PG Bouncer with this chart. Instead, to use PG bouncer, you can deploy PG bouncer with Postgres. For more information, refer to the `connectionPooler` section in https://github.com/Netcracker/pgskipper-operator/blob/main/charts/patroni-services/values.yaml.

The main Airflow deployment units are as follows:

* Airflow scheduler deployment deploys the Airflow scheduler pods
* Airflow API Server deployment deploys the Airflow UI
* Airflow DAG Processor deployment deploys DAG processor
* Airflow worker statefulset (deployment in case of stdout logging configuration) deploys Celery workers (only for Celery executor)
* Airflow flower deployment deploys a Web UI built on top of Celery to monitor Celery workers (only for Celery executor, deprecated)
* (optional) Ingress/route that exposes the service outside of the cloud.

# Prerequisites

The prerequisites for the installation are specified in the below sections.

## Common

The common prerequisites are specified below.

* External PostgreSQL server 
* External Redis (or external RabbitMQ)
* Precreated secrets/configmaps with additional configurations, if required

For integration with Qubership Monitoring, ensure that Qubership Monitoring is installed in the cluster and the cluster has monitoring entities defined CRDs for ServiceMonitor, PrometheusRule, and GrafanaDashboard. When deploying with a restricted user, check that the user has permissions for monitoring entities. 

For example:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: prometheus-monitored
rules:
- apiGroups:
  - monitoring.coreos.com
  resources:
  - servicemonitors
  - prometheusrules
  - podmonitors
  verbs:
  - create
  - get
  - list
  - patch
  - update
  - watch
  - delete
- apiGroups:
  - integreatly.org
  resources:
  - grafanadashboards
  verbs:
  - create
  - get
  - list
  - patch
  - update
  - watch
  - delete
```

And 

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
    name: prometheus-monitored
    namespace: airflow-namespace
roleRef:
    apiGroup: rbac.authorization.k8s.io
    kind: Role
    name: prometheus-monitored
subjects:
- kind: User
  name: airflow-deploy-user
```

* If the [Custom Preinstall Job](#custom-preinstall-job) is not used to create a PG database, then create the database in the External PostgreSQL server manually.

**Warning**: When you set the `webserver.defaultUser.enabled` parameter, the `airflow users create` command runs at the start of the Airflow pod. Ensure that the PostgreSQL database you are using does not have any conflicting records. For example, another user with the same email ID. It is possible to delete or recreate the user manually after the installation using the Airflow CLI. For more information, refer to [https://airflow.apache.org/docs/stable/cli-ref](https://airflow.apache.org/docs/stable/cli-ref).

### Restricted Deploy User with Site Manager

A restricted deploy user usually does not have permissions to create the cluster role bindings. Hence, it should be created manually as follows:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ .Release.Namespace }}-airflow-sm-auth-role-binding
  labels:
    {{- include "airflow-site-manager.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:auth-delegator
subjects:
- kind: ServiceAccount
  name: {{ include "airflow-site-manager.serviceAccountName" . }}
  namespace: {{ .Release.Namespace }}
```

In the above example, you should replace the following:

* `{{ .Release.Namespace }}` with the installation namespace 
* `{{- include "airflow-site-manager.labels" . | nindent 4 }}` with the site-manager labels (`airflow-site-manager.labels` installation parameter)
* `{{ include "airflow-site-manager.serviceAccountName" . }}` with the site manager service account name (`airflow-site-manager.serviceAccountName` installation parameter, by default `airflow-site-manager`)

After manually creating the role binding, during the installation, it is necessary to set `airflow-site-manager.siteManager.clusterRoleBinding.install` to "false".

## OpenShift

If you are using the OpenShift cloud with restricted SCC, the Airflow namespace must have specific annotations as follows:

```bash
oc annotate --overwrite namespace airflow openshift.io/sa.scc.uid-range="50000/50000"
oc annotate --overwrite namespace airflow openshift.io/sa.scc.supplemental-groups="50000/50000"
```

**Note**: When using restricted SCC, do not change the minimal number of supplemental-groups annotation. If you have other services installed in the Airflow installation namespace that require different minimal number in supplemental-groups annotation, you can set the `securityContexts.pod.fsGroup` and `statsd.securityContexts.pod.fsGroup` Airflow installation parameters to this minimal number and Airflow should work with different annotation.

**Note**: It is possible to deploy Airflow in the namespace without these annotations. In this case, it is necessary to overwrite the security context in deployment parameters. In the security context, `runAsUser` and `fsGroup` must be set to null. 

For example:

```yaml
securityContexts:
  pod:
    runAsNonRoot: true
    runAsUser: null
    fsGroup: null
    seccompProfile:
      type: RuntimeDefault
  containers:
    capabilities:
      drop:
        - ALL
    allowPrivilegeEscalation: false
```

## AWS

If required, create the ElasticCache and/or Amazon RDS PG (it is possible to use Aurora PostgreSQL) instances for Airflow. Note that HA is not supported for ElasticCache (it should be possible to specify custom [celery config](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#celery-config-options) for HA ElasticCache, but this is not tested by the platform).

# Best Practices and Recommendations

The recommended configurations that are tested are as follows:

* Celery Executor or Kubernetes executor is used.
* External Postgres/Redis is used.
* No persistence storage for DAGs is used. DAGs are added with the image, using gitSync or rclone.
* Persistence storage for logs is used.
* Triggerer is disabled.

**Note**: When installing Airflow, consider the `fernetKey` parameter. For more information, see [https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html). This parameter is used for the database encryption. When not specified in the **values.yaml** file, it is automatically generated as a random value by helm during the installation. In this case, after uninstalling Airflow, the key is lost and it would be impossible to recover the database. Hence, it is recommended to specify this parameter during the installation. It is also recommended to specify the `jwtSecret` parameter during the installation to avoid unnecessary pod restarts during chart upgrades.


## HWE

When configuring resources in Airflow, remember that resource usage in Airflow might depend on the number of DAGs, the complexity of the DAG structure, the DAG schedule and Airflow configuration parameters, for example, `workers.celery.instances`. Also, it is necessary to ensure that the services on which Airflow depends (Postgres/Redis/RabbitMQ) have enough resources.

Aside from container resources and pod replicas, main parameters that also affect the airflow performance are as follows:

|Parameter|Description|Default Value(as per chart, or airflow when not specified in the chart)|
|---|---|---|
|`config.core.parallelism`|This defines the maximum number of task instances that can run concurrently per scheduler in Airflow, regardless of the worker count. Generally, this value multiplied by the number of schedulers in your cluster, is the maximum number of task instances with the running state in the metadata database.|32|
|`config.core.dagbag_import_timeout`|The time before timing out a python file import.|30.0|
|`config.core.dag_file_processor_timeout`|The time before timing out a DagFileProcessor, which processes a dag file.|50|
|`config.scheduler.dag_dir_list_interval`|How often (in seconds) to scan the DAGs directory for new files.|300|
|`config.scheduler.max_dagruns_per_loop_to_schedule`|Number of DagRuns should a scheduler examine (and lock) when scheduling and queuing tasks.|20|
|`config.scheduler.max_dagruns_to_create_per_loop`|Maximum number of DAGs to create DagRuns for per scheduler loop.|10|
|`config.scheduler.max_tis_per_query`|This changes the batch size of queries in the scheduling main loop. This should not be greater than core.parallelism. If this is too high, SQL query performance may be impacted by the complexity of query predicate, and/or excessive locking. Additionally, you may hit the maximum allowable query length for your database. Set this to `0` to use the value of `config.core.parallelism`|16|
|`config.scheduler.parsing_processes`|The scheduler can run multiple processes in parallel to parse dags. This defines the number of processes that will run.|2|
|`config.celery.worker_concurrency`|The concurrency that will be used when starting workers with the airflow celery worker command. This defines the number of task instances that a worker will take, so size up your workers based on the resources on your worker box and the nature of your tasks.|16|
|`config.celery.sync_parallelism`|Number of processes CeleryExecutor uses to sync task state. `0` means to use max (1, number of cores - 1) processes.|0|
|`config.celery.worker_autoscale`|The maximum and minimum number of pool processes that will be used to dynamically resize the pool based on load. Enable autoscaling by providing max_concurrency, min_concurrency with the airflow celery worker command (always keep minimum processes, but grow to maximum if necessary). Pick these numbers based on resources on worker box and the nature of the task. If autoscale option is available, worker_concurrency are ignored. https://docs.celeryq.dev/en/latest/reference/celery.bin.worker.html#cmdoption-celery-worker-autoscale .|None|
|`config.database.sql_alchemy_pool_size`|The SqlAlchemy pool size is the maximum number of database connections in the pool. 0 indicates no limit.|5|
|`config.database.sql_alchemy_pool_recycle`|The SqlAlchemy pool recycle is the number of seconds a connection can be idle in the pool before it is invalidated. This config does not apply to sqlite. If the number of DB connections is ever exceeded, a lower config value will allow the system to recover faster.|1800|
|`config.database.sql_alchemy_max_overflow`|The maximum overflow size of the pool. When the number of checked-out connections reaches the size set in pool_size, additional connections are returned up to this limit. When those additional connections are returned to the pool, they are disconnected and discarded. It follows, then the total number of simultaneous connections the pool allows is pool_size + max_overflow, and the total number of “sleeping” connections the pool allows is `pool_size`. `max_overflow` can be set to -1 to indicate no overflow limit; no limit is placed on the total number of concurrent connections.|10|

Another thing that can affect resources is logging configuration. Too many log messages can affect the scheduler and worker container resources. What's more, when deploying Airflow with logging configuration, that also writes to filesystem, scheduler, and container pods include log groomer sidecars that are responsible for cleaning logs. The resource usage of these sidecars is also affected by logging configuration. Resource usage for the containers can be configured using `workers.logGroomerSidecar.resources` and `scheduler.logGroomerSidecar.resources parameters.`

Qubership platform provides 3 different reference resource profiles, but these profiles should be adjusted per project based on the number of DAGs, structure of DAGs, DAG schedule, and configuration options. 

**Note**: The profiles do not include storage size for logs (`workers.persistence.size` parameter), it should be configured based on your logging configuration, DAG number, DAG structure, and DAG schedule.

### Small

`Small` profile specifies the resources enough to start Airflow with no DAGs and with the following parameters set:

`config.core.parallelism`=8
`config.scheduler.max_tis_per_query`=4
`config.celery.worker_concurrency`=4

The profile resources are shown below:

| Container                             | CPU       | RAM, Mi | Number of containers |
|---------------------------------------|-----------|---------|----------------------|
| Scheduler                             | 0.4       | 768     | 1          |
| Scheduler log groomer sidecar(`*`)    | 0.1       | 256     | 1          |
| API server                            | 0.5       | 1024    | 1          |
| DAG Processor                         | 0.4       | 768     | 1          |
| DAG Processor log groomer sidecar(`*`)| 0.1       | 256     | 1          |
| Worker                                | 0.9       | 1920    | 1          |
| Worker log groomer sidecar(`*`)       | 0.1       | 256     | 1          |
| Migrate database job (`*`)(`**`)      | 0.5       | 512     | 1          |
| Create User Job (`*`)(`**`)           | 0.5       | 512     | 1          |
| Custom Preinstall Job (`*`)(`**`)     | 0.1       | 512     | 1          |
| GitSync(Rclone) sidecar (`*`)         | 0.2       | 512     | 2          |
| StatsD prometheus exporter (`*`)      | 0.1       | 256     | 1          |
| Status Provisioner (`**`)             | 0.2       | 256     | 1          |
| Airflow Site Manager (`*`)            | 0.2       | 256     | 1          |
| Integration Tests (`*`)(`**`)         | 0.2       | 256     | 1          |

Here `*` - optional container based on configuration, `**` - temporary container.

**Note**: The above resources are required for starting, not for working under load. For production, the resources should be increased.

## Medium

`Medium` profile specifies the approximate resources enough to run Airflow for dev purposes or for prod non-HA purposes with a small number of simple DAGs with the following parameters set to default values:

`config.core.parallelism`=32
`config.scheduler.max_tis_per_query`=16
`config.celery.worker_concurrency`=16

The profile resources are shown below:

| Container                             | CPU | RAM, Mi | Number of containers |
|---------------------------------------|-----|---------|----------------------|
| Scheduler                             | 1.0 | 1280    | 1          |
| Scheduler log groomer sidecar(`*`)    | 0.1 | 320     | 1          |
| API server                            | 0.5 | 2048    | 1          |
| DAG Processor                         | 1.0 | 1280    | 1          |
| DAG Processor log groomer sidecar(`*`)| 0.1 | 320     | 1          |
| Worker                                | 1.3 | 3072    | 1          |
| Worker log groomer sidecar(`*`)       | 0.1 | 320     | 1          |
| Migrate database job (`*`)(`**`)      | 1   | 1024    | 1          |
| Create User Job (`*`)(`**`)           | 0.5 | 512     | 1          |
| Custom Preinstall Job (`*`)(`**`)     | 0.1 | 512     | 1          |
| GitSync(Rclone) sidecar (`*`)         | 0.4 | 768     | 2          |
| StatsD prometheus exporter (`*`)      | 0.8 | 1024    | 1          |
| Status Provisioner (`**`)             | 0.2 | 512     | 1          |
| Airflow Site Manager (`*`)            | 0.2 | 512     | 1          |
| Integration Tests (`*`)(`**`)         | 0.2 | 512     | 1          |

Here `*` - optional container based on configuration, `**` - temporary container.

## Large

`Large` profile specifies the approximate resources enough to run Airflow for prod-HA purposes with the following parameters set to default values:

`config.core.parallelism`=24
`config.scheduler.max_tis_per_query`=12
`config.celery.worker_concurrency`=16

The profile resources are shown below:

| Container                            | CPU | RAM, Mi | Number of containers |
|--------------------------------------|-----|---------|----------------------|
| Scheduler                            | 1.2 | 2048    | 2                    |
| Scheduler log groomer sidecar(`*`)   | 0.1 | 320     | 1                    |
| API server                            | 0.8 | 2048    | 1                    |
| DAG Processor                         | 1.0 | 1536    | 2                    |
| DAG Processor log groomer sidecar(`*`)| 0.1 | 320     | 1                    |
| Worker                               | 1.3 | 3072    | 3                    |
| Worker log groomer sidecar(`*`)      | 0.1 | 320     | 1                    |
| Migrate database job (`*`)(`**`)     | 1   | 1024    | 1                    |
| Create User Job (`*`)(`**`)          | 0.5 | 512     | 1                    |
| Custom Preinstall Job (`*`)(`**`)    | 0.1 | 512     | 1                    |
| GitSync(Rclone) sidecar (`*`)        | 0.4 | 768     | 5                    |
| StatsD prometheus exporter (`*`)     | 0.8 | 1024    | 1                    |
| Status Provisioner (`**`)            | 0.2 | 512     | 1                    |
| Airflow Site Manager (`*`)           | 0.2 | 512     | 1                    |
| Integration Tests (`*`)(`**`)        | 0.2 | 512     | 1                    |

Here `*` - optional container based on configuration, `**` - temporary container.

# Parameters

Airflow platform uses community Airflow chart with some changes. The parameters for community chart can be found at [https://airflow.apache.org/docs/helm-chart/stable/parameters-ref.html](https://airflow.apache.org/docs/helm-chart/stable/parameters-ref.html).

List of modified/added files can be found at [README.md](/README.md). The changes from the community chart version are described below.

## Helm Parameter Configuration Changes from Community Version

The Helm chart works and uses the same parameters as defined in the community version at [https://github.com/apache/airflow/tree/main/chart](https://github.com/apache/airflow/tree/main/chart) and also includes some additional changes as follows:

* Custom preinstall job and its parameters are added. This job can be used to create a database for Airflow and Redis (with or without DBaaS).
* By default, DBaaS integration is enabled. This means that the parameters for DBaaS integration are passed to Airflow and the custom preinstall job.
* `data.metadataSecretName` and `data.brokerUrlSecretName` are set to `metadata-secret` and `broker-url-secret`.
* Airflow configuration environment variables related to broker/database connections are disabled using the `enableBuiltInSecretEnvVars.*` parameter.
* `extraSecrets.*` and `extraEnvFrom.*` parameters are used to pass environment variables related to the DBaaS integration.
* The `config.secrets.backend` parameter is set to custom secrets' backend with DBaaS integration (`qsdbaasintegration.dbaas_secrets_backend.DBAASSecretsBackend`).
* Parameters for direct Prometheus monitoring support are added. Monitoring is enabled by default, along with StatsD monitoring that comes with the Airflow chart.
* Default StatsD monitoring mappings are edited.
* Using internal Redis/PG is not supported.
* webserver_config.py example is included for integration with Keycloak in API server.
* Additional python logging config Airflow configuration is included. It is used by default and can be disabled by specifying the related parameters during the installation.
* By default, Airflow deploys with celery executor without persistence.
* Triggerer is disabled by default.
* It is possible to specify the number of Flower pods. (**Note**: This change was only made for DR support - specifying more than one flower pod would work incorrectly.)
* DR sitemanager subchart is added.
* Certificate object for integration with cert-manager is added.
* Default security context is set to be in line with the restricted Pod Security Standards. For more information, refer to [https://kubernetes.io/docs/concepts/security/pod-security-standards/](https://kubernetes.io/docs/concepts/security/pod-security-standards/).
* Added labels required by Qubership release.
* Status provisioner job and parameters for it are added.
* For scheduler, webserver and api-server deployments support of custom Qubership rolling update deployment strategies were added. The `useQubershipDeployerUpdateStrategies` parameter is added that can be used to disable Qubership update strategies (must be set to `false`).
* `rbac.create` parameter is set to `false` to avoid unnecessary for celery executor roles creation.

### Using Non-DBaaS Airflow Installation

By default with Qubership changes, Airflow deployment uses DBaaS integration. To install without DBaaS, some changes must be made in the installation parameters:

* Since `data.metadataSecretName` and `data.brokerUrlSecretName` are not set to `~` by default, if you want to use other data parameters, you must manually set `data.metadataSecretName` and `data.brokerUrlSecretName` to `~`.
* `enableBuiltInSecretEnvVars.AIRFLOW__CORE__SQL_ALCHEMY_CONN`, `enableBuiltInSecretEnvVars.AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `enableBuiltInSecretEnvVars.AIRFLOW_CONN_AIRFLOW_DB`, and `enableBuiltInSecretEnvVars.AIRFLOW__CELERY__BROKER_URL` parameters must be set to `true`.
* The `env` parameter must be set to `[]` or to the required value in the installation environments.
* `extraSecrets` must contain `{'dbaas-connection-params-main': ~}` to avoid unnecessary DBaaS secret creation. `extraEnvFrom` must be set to `~` or to the required value in your installation.
* `config.secrets.backend` must be set to `~`.
* `customPreinstallJob` must be reconfigured based on the installation requirements.

## Airflow Site Manager and DR Deployment

Airflow supports deployment in the active-standby DR scheme. [Airflow Site Manager](/docs/public/airflow-site-manager.md) is used for the DR deployment. 

* On an active site,  
  * Airflow components such as API Server, Scheduler, Workers, Flower are scaled up and should be deployed with replicas>0. The Flower component can be skipped.  
    Replicas' count should be the same as specified in the Airflow Site Manager's deployment parameters, namely, `FLOWER_REPLICAS`, `SCHEDULER_REPLICAS`, `API_SERVER_REPLICAS`, `DAG_PROCESSOR_REPLICAS` and `WORKER_REPLICAS`.
  * `airflow-site-manager.dr.mode` should be set to `active`.  
 
* On a standby site,   
  * Airflow components such as API Server, Scheduler, Workers, Flower are scaled down and should be deployed with replicas=0.
  * The PostgreSQL database creation job should be switched off:

```yaml
    customPreinstallJob:
    enabled: false # <----- switching off DB creation
    ...
    command: ~
    args: ["python", "/bin/createdb.py"]
    extraSecrets:
      'postgres-conn':
        stringData: |
          POSTGRES_HOST: {{ .Values.data.metadataConnection.host }}
          POSTGRES_PORT: "{{ .Values.data.metadataConnection.port }}"
          DB_NAME: {{ .Values.data.metadataConnection.db }}
          DB_USER: {{ .Values.data.metadataConnection.user }}
          POSTGRES_PASSWORD: {{ .Values.data.metadataConnection.pass }}
          POSTGRES_ADMIN_USER: usrname
          POSTGRES_ADMIN_PASSWORD: passwd
    extraEnvFrom: |
      - secretRef:
          name: 'postgres-conn'
```

  * DB migration should be switched off:

```yaml
    migrateDatabaseJob:
     enabled: false
 ```

  * `airflow-site-manager.dr.mode` should be set to `standby`.
* The database (PostgreSQL) and broker (Redis) settings, which are in the `data` section of the deployment parameters should be the same, except for `data.createPostgresDB`.

You can enable the Airflow Site Manager deployment by the Airflow `airflow-site-manager.enabled` deployment parameter. As the Airflow's chart is a parent chart, it can override the Airflow Site Manager deployment parameters.
All the possible parameters are listed below.

| Parameter                                                    | Mandatory                        | Type            | Default                                                                                | Description                                                                                                                                                                                                                 |
|--------------------------------------------------------------|----------------------------------|-----------------|----------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `airflow-site-manager.enabled`                               | false                            | bool            | `false`                                                                                | Enables the Airflow Site Manager deployment.                                                                                                                                                                                |
| `airflow-site-manager.image.repository`                      | false                            | string          | ghcr.io/netcracker/qubership-airflow-site-manager                                      | Docker images repository.                                                                                                                                                                                                   |
| `airflow-site-manager.image.pullPolicy`                      | false                            | string          | `"IfNotPresent"`                                                                       | The image pull policy.                                                                                                                                                                                                      |
| `airflow-site-manager.image.tag`                             | false                            | string          | `main`                                                                                 | This overrides the image tag.                                                                                                                                                                                               |
| `airflow-site-manager.imagePullSecrets`                      | false                            | list            | `[]`                                                                                   | The image pull secrets.                                                                                                                                                                                                     |
| `airflow-site-manager.siteManager.cr.install`                | false                            | bool            | `false`                                                                                | Deploy the site-manager CR.                                                                                                                                                                                                 |
| `airflow-site-manager.siteManager.clusterRoleBinding.install`| false                            | bool            | `true`                                                                                 | Deploy the site-manager Cluster Role Binding.                                                                                                                                                                                                 |
| `airflow-site-manager.siteManager.startAfter`                | true                             | list            | `[]`                                                                                   | The list of DR services Airflow depends on. Currently, it is PostgreSQL. The name of the service should be as in its site-manager CR. The Site Manager client renders services' names in the table of statuses.             |
| `airflow-site-manager.siteManager.timeout`                   | false                            | int             | `180`                                                                                  | The timeout site-manager waits for Airflow SM to respond.                                                                                                                                                                   |
| `airflow-site-manager.siteManager.tls.enabled`               | false                            | bool            | `false`                                                                                | Enable tls for SM.                                                                                                                                                                                                          |
| `airflow-site-manager.FLOWER_REPLICAS`                       | false                            | int             | `1`                                                                                    | The required number of `flower` replicas in the active mode. It must be 0 or 1.                                                                                                                                             |
| `airflow-site-manager.SCHEDULER_REPLICAS`                    | false                            | int             | `1`                                                                                    | The required number of `scheduler` replicas in the active mode.                                                                                                                                                             |
| `airflow-site-manager.API_SERVER_REPLICAS`                   | false                            | int             | `1`                                                                                    | The required number of `api server` replicas in the active mode.                                                                                                                                                            |
| `airflow-site-manager.DAG_PROCESSOR_REPLICAS`                | false                            | int             | `1`                                                                                    | The required number of `DAG processor` replicas in the active mode.                                                                                                                                                         |
| `airflow-site-manager.WORKER_REPLICAS`                       | false                            | int             | `1`                                                                                    | The required number of CeleryExecutor `worker` replicas in the active mode.                                                                                                                                                 |
| `airflow-site-manager.START_TIMEOUT`                         | false                            | int             | `120`                                                                                  | The timeout to wait for the Airflow Operator to start after it has been switched to the active mode.                                                                                                                        |
| `airflow-site-manager.SHUTDOWN_TIMEOUT`                      | false                            | int             | `25`                                                                                   | The timeout to wait for the Airflow Operator to scale to 0 after it has been switched to the standby mode.                                                                                                                  |
| `airflow-site-manager.RESOURCE_FOR_DR`                       | true                             | string          | '"" v1 configmaps airflow-dr-state'                                                    | A DRD parameter. The value must not be changed. It specifies configmap, which is used to store and manage the Airflow DR state. The configmap contains the DR mode and a state of the mode change.                          |                                                                                                                                                                      
| `airflow-site-manager.USE_DEFAULT_PATHS`                     | true                             | string          | `false`                                                                                | A DRD parameter. The value must not be changed. Airflow's DRD resource is a configmap and has its own parameters.                                                                                                           |
| `airflow-site-manager.DISASTER_RECOVERY_MODE_PATH`           | true                             | string          | `data.mode`                                                                            | A DRD parameter. The value must not be changed. It is the DRD configmap's parameter to store the Airflow DR mode.                                                                                                           |
| `airflow-site-manager.DISASTER_RECOVERY_NOWAIT_PATH`         | true                             | string          | `data.noWait`                                                                          | A DRD parameter. The value must not be changed. This parameter specifies the path to the disaster recovery no-wait field in the Airflow DR configmap.                                                                       |
| `airflow-site-manager.DISASTER_RECOVERY_STATUS_MODE_PATH`    | true                             | string          | `data.status_mode`                                                                     | A DRD parameter. The value must not be changed. This parameter specifies the path to the disaster recovery status `mode` field in the Airflow DR configmap.                                                                 |
| `airflow-site-manager.DISASTER_RECOVERY_STATUS_STATUS_PATH`  | true                             | string          | `data.status_status`                                                                   | A DRD parameter. The value must not be changed. This parameter specifies the path to the disaster recovery status `status` field in the Airflow DR configmap.                                                               |
| `airflow-site-manager.DISASTER_RECOVERY_STATUS_COMMENT_PATH` | true                             | string          | `data.status_comment`                                                                  | A DRD parameter. The value must not be changed. This parameter specifies the path to the disaster recovery status `comment` field in the Airflow DR configmap. The comment contains information about the last mode change. |                                                                                                                                                                                                                |
| `airflow-site-manager.DISASTER_RECOVERY_NOWAIT_AS_STRING`    | true                             | string          | `true`                                                                                 | A DRD parameter. The value must not be changed. If this parameter is true, the DRD uses string values for the no-wait parameter, otherwise a Boolean value is used.                                                         |
| `airflow-site-manager.TREAT_STATUS_AS_FIELD`                 | true                             | string          | `true`                                                                                 | A DRD parameter. The value must not be changed. It specifies that a configmap is used as a DRD resource instead of a CR.                                                                                                    |
| `airflow-site-manager.HEALTH_MAIN_SERVICES_ACTIVE`           | true                             | string          | `deployment airflow-airflow-webserver`                                                 | A DRD parameter. It must not be empty. This parameter is not used in the Airflow DR, but is required to be set by DRD.                                                                                                      |
| `airflow-site-manager.HTTP_AUTH_ENABLED`     | false                            | boolean          | `false`                                                                          | A DRD parameter. It specifies if the authentication should be enabled.                                        |
| `airflow-site-manager.SM_SECURE_AUTH`     | false                            | boolean          | `false`                                                                          | A DRD parameter. It specifies whether the smSecureAuth mode is enabled for Site Manager.                                   |
| `airflow-site-manager.SITE_MANAGER_SERVICE_ACCOUNT_NAME`     | false                            | string          | `sm-auth-sa`                                                                           | A DRD parameter. It specifies the name of the service account used by site-manager. It should be set along with `SITE_MANAGER_NAMESPACE` to enable JWT token checks for DR requests.                                        |
| `airflow-site-manager.SITE_MANAGER_NAMESPACE`                | false                            | string          | `site-manager`                                                                         | A DRD parameter. It specifies the namespace where Site-Manager of K8s' clusters are deployed. It should be set along with `SITE_MANAGER_SERVICE_ACCOUNT_NAME` to enable JWT token checks for DR requests.                   |
| `airflow-site-manager.SITE_MANAGER_CUSTOM_AUDIENCE`          | false                            | string          | `sm-services`                                                                         | A DRD parameter. It specifies the name of custom audience for the rest api token, that is used to connect with services. It is necessary if Site Manager installed with `SM_SECURE_AUTH=true`, and has applied custom audience (`sm-services` by default). It is considered if `airflow-site-manager.SM_SECURE_AUTH` parameter is set to `true`.                   |
| `airflow-site-manager.dr.mode`                               | true                             | string          | `standby`                                                                              | The DR mode of Airflow. It should be `active` or `standby`.                                                                                                                                                                 |
| `airflow-site-manager.dr.noWait`                             | false                            | bool            | `false`                                                                                | It specifies whether it is a failover or not.                                                                                                                                                                               |
| `airflow-site-manager.securityContexts.pod`                  | false                            | object          | `{}`                                                                                   | The Airflow Site Manager pod security context.                                                                                                                                                                              |
| `airflow-site-manager.securityContexts.container`            | false                            | object          | `{}`                                                                                   | The Airflow Site Manager container security context.                                                                                                                                                                        |
| `airflow-site-manager.service.type`                          | false                            | string          | `ClusterIP`                                                                            | The type of the Kubernetes service.                                                                                                                                                                                         |
| `airflow-site-manager.service.port`                          | false                            | string          | `8080`                                                                                 | Do not change it. The Site Manager uses the 8080 port.                                                                                                                                                                      |
| `airflow-site-manager.ingress`                               | false                            | object          | `{}`                                                                                   | Set the proper `host` if ingress is enabled.                                                                                                                                                                                |
| `airflow-site-manager.ingress.enabled`                       | false                            | bool            | false                                                                                  | Enables the Airflow Site Manager ingress creation.                                                                                                                                                                          |
| `airflow-site-manager.ingress.hosts`                         | Mandatory if ingress is enabled.  | list ob objects | -                                                                                      | The Ingress host. It should be compatible with the cluster's ingress URL routing rules.                                                                                                                                     |
| `airflow-site-manager.ingress.path`                          | Mandatory if ingress is enabled. | string          | -                                                                                      | It should be set to '/'.                                                                                                                                                                                                    |
| `airflow-site-manager.logLevel`                              | false                            | int             | `2`                                                                                    | Set higher levels up to 4 for more verbose logging.                                                                                                                                                                         |
| `airflow-site-manager.resources`                             | false                            | object          | `{}`                                                                                   | The pod resource requests and limits.                                                                                                                                                                                       |
| `airflow-site-manager.labels`                                | false                            | object          | `app.kubernetes.io/part-of: 'airflow'`                                                 | Labels to add to Site Manager objects.                                                                                                                                                                                      |
| `airflow-site-manager.priorityClassName`                     | false                            | string          | `~`                                                                                    | Priority class name for airflow site manager.                                                                                                                                                                               |

Following is an example of Airflow Site Manager deployment parameters:

```yaml
airflow-site-manager:
  enabled: true
  siteManager:
    cr:
      install: true
    startAfter: ["postgres"]
    timeout: 180
  HTTP_AUTH_ENABLED: true
  SM_SECURE_AUTH: false
  SITE_MANAGER_SERVICE_ACCOUNT_NAME: sm-auth-sa
  SITE_MANAGER_NAMESPACE: site-manager
  START_TIMEOUT: 120
  SHUTDOWN_TIMEOUT: 25
  FLOWER_REPLICAS: 1
  DAG_PROCESSOR_REPLICAS: 1
  SCHEDULER_REPLICAS: 1
  API_SERVER_REPLICAS: 1
  WORKER_REPLICAS: 2
  logLevel: 2
  ingress:
    enabled: true
    annotations: {}
    hosts:
      - host: "airflow-site-manager.your.cluster.qubership.com"
        paths:
          - path: /
            backend:
              servicePort: 8080
```

**Note**: There is another option for PG replication that can be used with Hadoop deployments. For more information, see [Airflow HDFS DR Sync DAG](/docs/public/dr-sync-dag.md).

## Custom Preinstall Job

**Note**: To disable the job, set `customPreinstallJob.enabled` to false.

In addition to official Airflow helm chart, this helm chart provides a custom preinstall job that is based on helm pre-install hook and can execute custom command before the Airflow installation. For more information, refer to [https://helm.sh/docs/topics/charts_hooks/](https://helm.sh/docs/topics/charts_hooks/). 
It is possible to add custom permissions using a Kubernetes role for this job if needed (for example, if the job is used to create Kubernetes objects). The job supports the following parameters:

|Name|Description|
|---|---|
|customPreinstallJob.enable|Specifies if the custom predeploy job is deployed.|
|customPreinstallJob.labels|Specifies the podLabels for the job. They are added to global labels and to labels required by the cloud release.|
|customPreinstallJob.resources|Specifies the resources for the job.|
|customPreinstallJob.extraSecrets|Specifies the extra secrets that can be deployed with the job.|
|customPreinstallJob.extraConfigMaps|Specifies the extra configuration maps that can be deployed with the job.|
|customPreinstallJob.extraVolumes|Specifies the extra volumes for the job container.|
|customPreinstallJob.extraVolumeMounts|Specifies the extra volume mounts for the job container.|
|customPreinstallJob.env|Specifies the environment for the job container.|
|customPreinstallJob.extraEnvFrom|Specifies the extraEnvFrom for the job container.|
|customPreinstallJob.serviceAccount.create|Specifies if the service account for the job should be created.|
|customPreinstallJob.role.create|Specifies if the role for the job should be created.|
|customPreinstallJob.role.rules|Specifies the rules for the job role.|
|customPreinstallJob.command|Specifies the command for the job.|
|customPreinstallJob.args|Specifies the arguments for the job.|

By default, the parameters for [DBaaS to Create Postgres and Redis Databases](#dbaas-integration) are set with Qubership release requirements. These parameters need modification based on your cloud setup:

```yaml
customPreinstallJob:
  enabled: true
  labels: {}
  resources: {}
  serviceAccount:
    create: true
  role:
    create: true
    rules:
      - apiGroups:
          - ""
        resources:
          - secrets
        verbs:
          - create
          - get
          - delete
          - update
  securityContexts:
    pod: {}
    container: {}
  command: ~
  args: ["python", "/bin/create_dbs_dbaas.py"]
  extraSecrets:
    'dbaas-connection-params-preins':
      stringData: |
        DBAAS_HOST: 'insert.api.dbaas.addres.here.svc'
        DBAAS_USER: 'insert dbaas user here'
        DBAAS_PASSWORD: 'insert dbaas password here'
        DBAAS_PG_DB_OWNER: 'insert dbaas pg owner here'
        DBAAS_PG_BACKUP_DISABLED: 'true'
        DBAAS_PG_MICROSERVICE_NAME: 'insert pg microservice name here'
        DBAAS_REDIS_DB_OWNER: 'insert dbaas redis password here'
        DBAAS_REDIS_BACKUP_DISABLED: 'true'
        DBAAS_REDIS_MICROSERVICE_NAME: 'insert redis microservice name here'
        AIRFLOW_EXECUTOR: '{{ .Values.executor }}'
  extraEnvFrom: |
    - secretRef:
        name: 'dbaas-connection-params-preins'
```

The possible use cases that are implemented on the Qubership side are described below.

### Precreating PostgreSQL Database

You can use the custom preinstall job to create Postgres database during the Airflow installation. Platform provides a [python script](/docker/createdb.py) that creates a database and user in the PG database using administrator credentials. For this script to work, it is required to use the following parameters:

```yaml
customPreinstallJob:
  enabled: true
  resources: {}
  env: []
  serviceAccount:
    create: false
  role:
    create: false
  command: ~
  args: ["python", "/bin/createdb.py"]
  extraSecrets:
    'postgres-conn':
      stringData: |
        POSTGRES_HOST: {{ .Values.data.metadataConnection.host }}
        POSTGRES_PORT: "{{ .Values.data.metadataConnection.port }}"
        DB_NAME: {{ .Values.data.metadataConnection.db }}
        DB_USER: {{ .Values.data.metadataConnection.user }}
        POSTGRES_PASSWORD: {{ .Values.data.metadataConnection.pass }}
        POSTGRES_ADMIN_USER: usrname
        POSTGRES_ADMIN_PASSWORD: passwd
  extraEnvFrom: |
    - secretRef:
        name: 'postgres-conn'
```

### DBaaS Integration

You can use the custom preinstall job to create Postgres and Redis databases using DBaaS during the Airflow installation. Platform provides a reference [python script](/docker/create_dbs_dbaas.py) that creates the databases using DBaaS. For this script to work, use the following parameters (set by default):

```yaml
...
customPreinstallJob:
  enabled: true
  labels: {}
  resources: {}
  serviceAccount:
    create: true
  role:
    create: true
    rules:
      - apiGroups:
          - ""
        resources:
          - secrets
        verbs:
          - create
          - get
          - delete
          - update
  securityContexts:
    pod: {}
    container: {}
  command: ~
  args: ["python", "/bin/create_dbs_dbaas.py"]
  extraSecrets:
    'dbaas-connection-params-preins':
      stringData: |
        DBAAS_HOST: 'insert.api.dbaas.addres.here.svc'
        DBAAS_USER: 'insert dbaas user here'
        DBAAS_PASSWORD: 'insert dbaas password here'
        DBAAS_PG_DB_OWNER: 'insert dbaas pg owner here'
        DBAAS_PG_BACKUP_DISABLED: 'true'
        DBAAS_PG_MICROSERVICE_NAME: 'insert pg microservice name here'
        DBAAS_REDIS_DB_OWNER: 'insert dbaas redis password here'
        DBAAS_REDIS_BACKUP_DISABLED: 'true'
        DBAAS_REDIS_MICROSERVICE_NAME: 'insert redis microservice name here'
        AIRFLOW_EXECUTOR: '{{ .Values.executor }}'
  extraEnvFrom: |
    - secretRef:
        name: 'dbaas-connection-params-preins'
```

Platform also provides a DBaaS integration package for Airflow that [implements](/docker/dbaasintegrationpackage/qsdbaasintegration/dbaas_secrets_backend.py) Airflow custom secrets' backend. For more information, refer to [https://airflow.apache.org/docs/apache-airflow/3.0.2/security/secrets/secrets-backend/index.html](https://airflow.apache.org/docs/apache-airflow/3.0.2/security/secrets/secrets-backend/index.html). It is intended to be used with the custom preinstall job DBaaS script. The custom secrets' backend gets Redis and PG connections for Airflow from DBaaS. To enable custom secrets' backend, the following parameters must be specified (set by default):

```yaml
...
env:
  - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_SECRET
    value: airfow_pg_main_conn
  - name: AIRFLOW__CORE__SQL_ALCHEMY_CONN_SECRET
    value: airfow_pg_main_conn
  - name: AIRFLOW__CELERY__BROKER_URL_SECRET
    value: airfow_celery_redis_main_conn
...
enableBuiltInSecretEnvVars:
  AIRFLOW__CORE__SQL_ALCHEMY_CONN: false
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: false
  AIRFLOW_CONN_AIRFLOW_DB: false
  AIRFLOW__CELERY__BROKER_URL: false
...
extraSecrets:
  'dbaas-connection-params-main':
    stringData: |
      DBAAS_HOST: 'insert.api.dbaas.addres.here.svc'
      DBAAS_USER: 'insert dbaas user here'
      DBAAS_PASSWORD: 'insert dbaas password here'
      DBAAS_PG_DB_OWNER: 'insert dbaas pg owner here'
      DBAAS_PG_BACKUP_DISABLED: 'true'
      DBAAS_PG_MICROSERVICE_NAME: 'insert pg microservice name here'
      DBAAS_REDIS_DB_OWNER: 'insert dbaas redis password here'
      DBAAS_REDIS_BACKUP_DISABLED: 'true'
      DBAAS_REDIS_MICROSERVICE_NAME: 'insert redis microservice name here'
      AIRFLOW_EXECUTOR: '{{ .Values.executor }}'
      MAAS_HOST: 'insert.api.maas.addres.here.svc'
      MAAS_USER: 'insert maas user here'
      MAAS_PASSWORD: 'insert maas password here'
...
extraEnvFrom: |
  - secretRef:
      name: 'dbaas-connection-params-main'
...
workers:
...
  livenessProbe:
    enabled: false
...
config:
...
  secrets:
    backend: qsdbaasintegration.dbaas_secrets_backend.DBAASSecretsBackend


```

In the above example, MAAS parameters are not needed, if MAAS integration is not used.

**Note**: By default, the `DBAAS_PG_DB_NAME_PREFIX` environment variable is not set. This means that the database name is provided by the DBaaS aggregator. The `DBAAS_PG_DB_NAME_PREFIX` environment variable can be used to set the prefix for the PG database name.

**Note**: For SSL configuration, see [Specifying SSL Connections to Redis and Postgres](#specifying-ssl-connections-to-redis-and-postgres).

**Note**: When DBAAS/MAAS has TLS enabled on API, it is possible to use `DBAAS_API_VERIFY` environment variable for preinstall job and airflow containers to configure TLS verification. By default, it is enabled, but it is possible to set it to `False` in order to disable cert verification. It is also possible to set it to custom path in order to use custom certificate for verification. Alternatively, it is possible to use `REQUESTS_CA_BUNDLE` and `SSL_CERT_FILE` envs, but it will affect all python requests.

## DBaaS Integration for Airflow Connections

With the [platform-provided DBaaS integration package for Airflow](/docker/dbaasintegrationpackage/qsdbaasintegration/dbaas_secrets_backend.py), it is also possible to get Airflow connections from DBaaS (**at the moment, only Postgres connections are supported**). To do so, it is necessary to specify the `config.backend.backend_kwargs` parameter, along with `config.backend.kwargs`. In `config.backend.backend_kwargs`, it is necessary to specify the [data for DBaaS request](https://github.com/Netcracker/qubership-dbaas/blob/main/docs/rest-api.md#get-or-create-new-database) in JSON format as a string. The data should be specified in the JSON field with "${connection_name}_dbaas" name. The `config.backend.backend_kwargs` parameter supports helm templating, so you can specify the data for DBaaS request separately in yaml format and convert it to JSON using helm functions. For example, for connections `postgres_test_conn_1` and `postgres_test_conn_2`:

```yaml
qs_secrets_backend_params:
  postgres_test_conn_1_dbaas:
    namePrefix: conn_test_1
    backupDisabled: "true"
    type: postgresql
    dbOwner: conn_test1
    originService: conn_test1
    classifier:
      namespace: airflow-test
      scope: service
      microserviceName: conn_test1
      isServiceDb: "true"
  postgres_test_conn_2_dbaas:
    namePrefix: conn_test_2
    backupDisabled: "true"
    type: postgresql
    dbOwner: conn_test2
    originService: conn_test2
    classifier:
      namespace: airflow-test
      scope: service
      microserviceName: conn_test2
      isServiceDb: "true"
config:
  secrets:
    backend: qsdbaasintegration.dbaas_secrets_backend.DBAASSecretsBackend # used by default
    backend_kwargs: "{{ .Values.qs_secrets_backend_params | toJson }}"
```

**Note**: By default, if the database does not exist, the request to DBaaS goes to the `/api/v3/dbaas/{airflow_namespace}/databases` address, where `{airflow_namespace}` is the Airflow installation namespace. It is possible to change the request address to the namespace specified in the classifier by setting the `DBAAS_CONN_NAMESPACE_FROM_CONFIG` environment variable to "true".

**Note**: It is possible to enable more logging in the DBaaS integration package by setting the `DBAAS_INTEGRATION_LOG_LEVEL` environment variable to `DEBUG`. The `config.logging.logging_level` parameter must also be set to debug in this case.

### MaaS Integration for Airflow Connections

[Platform-provided DBaaS integration package for Airflow](/docker/dbaasintegrationpackage/qsdbaasintegration/dbaas_secrets_backend.py) also allows to get Kafka connections from MaaS. The approach works mostly the same as with DBaaS connection, but the structure of JSON connection config is a bit different. To get Kafka connection, it is necessary to specify the data [for MaaS request](https://github.com/Netcracker/qubership-maas/blob/main/docs/rest-api.md#get-or-create-kafka-topic) to `{maas_host}/api/v1/kafka/topic` in the `maas_request_data` field, additional properties for connection in the `connection_properties` field (is used to fill the extra field of the connection, can be used to overwrite the properties received from MaaS) and the connection type in the `maas_type` field. All these three fields should be added to the field with "${connection_name}_maas" name. 

Following is an example for connection with name `kafka_test_conn`:

```yaml
qs_secrets_backend_params:
  kafka_test_conn_maas:
    maas_request_data:
      instance: kafkaone
      name: airflowtestkafkatestopic
      replicationFactor: 3
      retention.ms: 1000
      classifier:
        namespace: local
        name: airflowtestkafkatestopic
    connection_properties:
      group.id: testgroup
      auto.offset.reset: earliest
      enable.auto.offset.store: false
      session.timeout.ms: 30000
    maas_type: kafka
config:
  secrets:
    backend: qsbaasintegration.dbaas_secrets_backend.DBAASSecretsBackend # used by default
    backend_kwargs: "{{ .Values.qs_secrets_backend_params | toJson }}"
```

Since Airflow Kafka connection does not include topic, the topic name is not passed to the connection. The following fields are taken from the MaaS response and added to the Kafka connection extra field: `bootstrap.servers`, `sasl.username`, `sasl.password`, `sasl.mechanism`, `security.protocol`.

**Note**: By default, the `X-Origin-Namespace` header of the MaaS request is the same as the namespace in MaaS classifier. To use the Airflow namespace, it is possible to set the `MAAS_CONN_NAMESPACE_FROM_CONFIG` environment variable to `false`.

**Note**: As with DBaaS, additional logging can be enabled by setting the `DBAAS_INTEGRATION_LOG_LEVEL` environment variable to `DEBUG`. The `config.logging.logging_level` parameter must also be set to debug in this case.

## Creating a Secret for Custom Keycloak Client Registration

If you have a custom Keycloak that creates clients based on kubernetes secrets, it can be done using the `extraSecrets` parameter. 

For example,

```yaml
extraSecrets:
  'airflow3-client-credentials':
    labels:
      core.qubership.com/secret-type: m2m
    stringData: |
      name: 'airflow3'
      username: 'airflow3'
      password: 'airflow_password'
```

**Note**: The `extraSecrets` parameter supports templating, so it is possible to generate a random password.

```yaml
extraSecrets:
  'airflow3-client-credentials':
    labels:
      core.qubership.com/secret-type: m2m
    stringData: |
      name: 'airflow3'
      username: 'airflow3'
      password: '{{ randAlphaNum 64 }}'
```

If needed, it is possible to access the created secret within Airflow pods. For example, using the `extraEnv` parameter, it is possible to access the secret values as environment variables.

```yaml
extraEnv: |
      - name: M2M_SECRET_USERNAME
        valueFrom:
          secretKeyRef:
            name: airflow3-client-credentials
            key: username
```

## Specifying External Broker

You can use Airflow with Celery executor with Redis or RabbitMQ. To connect to external Redis, you must specify the `data.brokerUrl` parameter, for example `redis://default:redis2@redis:6379/2`.

To use external RabbitMQ, the `data.brokerUrl` parameter must be specified, for example `amqp://admin:admin@rabbitmq.rabbitmq.svc:5672`.

**Note**: If you do not want to use default RabbitMQ vhost, you can specify it too, for example, `amqp://admin:admin@rabbitmq.rabbitmq.svc:5672/custom_vhost_name`, however this vhost must be manually created.

## Specifying SSL Connections to Redis and Postgres

For some SSL PG/Redis configurations, SSL certificates must be available inside some Airflow pods. You can add certificates to Airflow pods during image building or using `extraSecrets` and `{{component name}}.extraVolumes`/`{{component name}}.extraVolumeMounts` parameters. For example, mounting certificates in workers:

```yaml
extraSecrets:
  sslkey:
    data: >
      root.crt:
      certcontent
  rediskey:
    data: >
      cacert.crt:
      certcontent
...
workers:
  extraVolumeMounts:
    - name: sslkey
      mountPath: /home/airflow/certs/root.crt
      subPath: root.crt
      readOnly: true
    - name: rediskey
      mountPath: /home/airflow/certs/cacert.crt
      subPath: cacert.crt
      readOnly: true      
  extraVolumes:
    - name: sslkey
      secret:
        secretName: sslkey
    - name: rediskey
      secret:
        secretName: rediskey  
```

**Note**: It is possible to use `volumes` and `volumeMounts` parameters instead. They add volumes/volumemounts for all Airflow containers.

### Redis Connection Configuration

Two different helm chart parameters allow specifying Redis connection separately:

* `data.brokerUrlSecretName` - This parameter allows specifying the Redis secret to use. The secret must contain the `connection` field with Redis connection.
* `data.brokerUrl` - This parameter allows specifying the Redis connection. To use it, `data.brokerUrlSecretName` must be set to `~`.

**For using Redis without SSL, the connection must look like the following**:

* `redis://user:password@redis.host.addres:6379`

**For using Redis with SSL but without CA check, the connection must look like the following**:

* `rediss://user:password@redis.host.addres:6379`
or
* `rediss://user:password@redis.host.addres:6379?ssl_cert_reqs=CERT_NONE`

**For using Redis with SSL and with CA check, the connection must look like the following**:

* `rediss://user:password@redis.host.addres:6379?ssl_cert_reqs=CERT_REQUIRED`

**For using Redis with SSL and with CA check and specifying a local certificate for checking, the connection must look like the following**:

* `rediss://user:password@redis.host.addres:6379?ssl_cert_reqs=CERT_REQUIRED&ssl_ca_certs=/local/path/to/cacert.pem`

**Note**: When using SSL Redis CA check, a certificate can be added only to worker/scheduler pods.

### Postgres Connection Configuration

Two different helm chart parameters allow specifying the Postgres connection separately:

* `data.metadataSecretName`—This parameter allows specifying the PG secret to use. The secret must contain the `connection` field with PG connection.
* `data.metadataConnection`—This parameter allows specifying PG connection parameters. To use it, `data.metadataSecretName` must be set to `~`.

For SSL configuration, the `data.metadataConnection.sslmode` parameter is responsible (or the last part of PG connection, for example, `postgresql://user:password@pg.url.address:5432/dbname?sslmode=verify-ca`).

* For using PG without SSL, this parameter must be set to `disable`. 
* For enabling SSL but disabling CA check, this parameter must be set to `require`. 
* For enabling SSL with CA check, this parameter must be set to `verify-ca` (CA certificate can be added to the /home/airflow/.postgresql/root.crt file).
* For enabling SSL with CA check and custom local certificate location, this parameter must be set to `verify-ca&sslrootcert=/path/to/local/cert/root.crt`.

**Note**: When using SSL PG with CA check, a certificate should be added to all Airflow component pods.

### Getting CA Certificate From Cert-Manager

It is possible to create a secret containing ca-cert using [cert-manager](https://cert-manager.io/). To do so, the following configuration is available:

```yaml
certManagerInegration:
  ## Enabling
  enabled: false
  ## Secret name
  secretName: airflow-services-tls-certificate
  duration: 365
  ## for future use
  subjectAlternativeName:
    additionalDnsNames: [ ]
    additionalIpAddresses: [ ]
  ## cluster issuer with CA
  clusterIssuerName: ~
```

Note that in this case, cert-manager must be installed in the cluster. In case `certManagerIntegration.enabled` is set to `true`, the cert manager creates a secret containing ca-cert from clusterIssuer with `certManagerInegration.clusterIssuerName` in the ca.crt field with the name, `certManagerInegration.secretName`. For example, to create a secret with `ca.crt` and use it for Redis connection certificate verification, you must specify the the following parameters:

```yaml
...
data:
  metadataConnection:
...
  brokerUrl: rediss://default:redispwd@dbaas-airflow2.redis:6379?ssl_cert_reqs=CERT_REQUIRED&ssl_ca_certs=/home/airflow/certs/ca.crt
...
workers:
  extraVolumeMounts:
    - name: rediskey
      mountPath: /home/airflow/certs/ca.crt
      subPath: ca.crt
      readOnly: true      
  extraVolumes:
    - name: rediskey
      secret:
        secretName: airflow-services-tls-certificate
...
certManagerInegration:
  enabled: true
  secretName: airflow-services-tls-certificate
  duration: 365
  subjectAlternativeName:
    additionalDnsNames: [ ]
    additionalIpAddresses: [ ]
  clusterIssuerName: qa-issuer-self

```

### Using SSL with Connections Received from DBaaS

For Redis and PG connections, it is possible to use SSL when connections are received from DBaaS. By default, when Redis or PG connections have TLS enabled, the certificate verification is disabled. To enable it, it is possible to use the `DBAAS_SSL_VERIFICATION_MAIN` environment variable that should be available in Airflow pods.

|`DBAAS_SSL_VERIFICATION_MAIN` value|Postgres connections effect|Redis connections effect|
|---|---|---|
|`DISABLED`|No certificate check|No certificate check|
|`ENABLED`|Certificate check based on default PG trusted roots' location (`/home/airflow/.postgresql/root.crt`) needs to be added to the image|Use the default redis python client trusted roots|
|`CERT_PATH:system`|Certificate check based on the default OS trusted roots|Use the default redis python client trusted roots|
|`CERT_PATH:/custom/certificate/path`|Certificate check based on custom trusted roots|Certificate check based on custom trusted roots|

For example, to use the cert-manager certificate for SSL verification, the parameters can be specified as follows:

```yaml
extraSecrets:
  'dbaas-connection-params-main':
    stringData: |
      DBAAS_HOST: 'insert.api.dbaas.addres.here.svc'
      DBAAS_USER: 'insert dbaas user here'
      DBAAS_PASSWORD: 'insert dbaas password here'
      DBAAS_PG_DB_OWNER: 'insert dbaas pg owner here'
      DBAAS_PG_BACKUP_DISABLED: 'true'
      DBAAS_PG_MICROSERVICE_NAME: 'insert pg microservice name here'
      DBAAS_REDIS_DB_OWNER: 'insert dbaas redis password here'
      DBAAS_REDIS_BACKUP_DISABLED: 'true'
      DBAAS_REDIS_MICROSERVICE_NAME: 'insert redis microservice name here'
      AIRFLOW_EXECUTOR: '{{ .Values.executor }}'
      MAAS_HOST: 'insert.api.maas.addres.here.svc'
      MAAS_USER: 'insert maas user here'
      MAAS_PASSWORD: 'insert maas password here'
      DBAAS_SSL_VERIFICATION_MAIN: 'CERT_PATH:/home/airflow/certs/ca.crt'
certManagerInegration:
  enabled: true
  secretName: airflow-services-tls-certificate
  duration: 365
  subjectAlternativeName:
    additionalDnsNames: [ ]
    additionalIpAddresses: [ ]
  clusterIssuerName: common-cluster-issuer
volumeMounts:
  - name: commoncert
    mountPath: /home/airflow/certs/ca.crt
    subPath: ca.crt
    readOnly: true      
volumes:
  - name: commoncert
    secret:
      secretName: airflow-services-tls-certificate
```

## Enabling TLS on airflow UI inside kubernetes

It is possible to enable TLS on Airflow web UI directly inside kubernetes. For this, Airflow API server needs TLS key and certificate. TLS key and certificate can be requested from cert-manager using `certManagerInegration.enabled` parameter. By default, it creates the secret `airflow-services-tls-certificate` with TLS certificate, TLS key and CA certificate. Alternatively, TLS key and certificate can be specified using `extraSecrets` parameter. After this, it is necessary to mount the certificates into webserver pod and specify the certificates in API server using `config.webserver.web_server_ssl_cert` and `config.webserver.web_server_ssl_key` (or `config.api.ssl_cert` and `config.api.ssl_key`). Also, it is necessary to specify the HTTPS scheme for API server liveness, readiness, and startup probes. Since in Airflow 3 workers go to airflow API server, workers must also have access to the TLS certificate and `execution_api_server_url` must be redirected to https. If using kubernetes with NGINX ingress controller, it is possible to pass annotations for ingress controller to work with TLS backend, for example:

```yaml
# airflow-install-namespace here should be replaced with the namespace where airflow is installed.
ingress:
  apiServer:
    annotations:
      nginx.ingress.kubernetes.io/backend-protocol: HTTPS
      nginx.ingress.kubernetes.io/proxy-ssl-verify: 'on'
      nginx.ingress.kubernetes.io/proxy-ssl-name: airflow-webserver.airflow-install-namespace # airflow-webserver must be replaced with ${{airflowname}}-webserver.
      nginx.ingress.kubernetes.io/proxy-ssl-secret: airflow-install-namespace/airflow-services-tls-certificate
workers:
  extraVolumeMounts:
    - name: certs
      mountPath: /home/airflow/certs/
      readOnly: true
  extraVolumes:
    - name: certs
      projected:
        sources:
          - secret:
              name: airflow-services-tls-certificate
  env:
   - name: REQUESTS_CA_BUNDLE
     value: /home/airflow/certs/ca.crt
   - name: SSL_CERT_FILE
     value: /home/airflow/certs/ca.crt
apiServer:
  extraVolumeMounts:
    - name: certs
      mountPath: /home/airflow/certs/
      readOnly: true
  extraVolumes:
    - name: certs
      projected:
        sources:
          - secret:
              name: airflow-services-tls-certificate
  livenessProbe:
    scheme: HTTPS
  readinessProbe:
    scheme: HTTPS
  startupProbe:
    scheme: HTTPS
config:
  core:
    execution_api_server_url:  https://airflow-api-server:8080/execution/
  webserver:
    web_server_ssl_cert: /home/airflow/certs/tls.crt
    web_server_ssl_key: /home/airflow/certs/tls.key
certManagerInegration:
  clusterIssuerName: common-cluster-issuer
  enabled: true
```

For enabling TLS on ingress itself, please refer to [Enabling HTTPS for Airflow Ingresses](#enabling-https-for-airflow-ingresses).

### Re-encrypt Route In Openshift Without NGINX Ingress Controller

Automatic re-encrypt Route creation is not supported out of box, need to perform the following steps:

1. Disable Ingress in deployment parameters: `ingress.apiServer.enabled: false`.

   Deploy with enabled apiServer Ingress leads to incorrect Ingress and Route configuration.

2. Create Route manually. You can use the following template as an example:

   ```yaml
   kind: Route
   apiVersion: route.openshift.io/v1
   metadata:
     annotations:
       route.openshift.io/termination: reencrypt
     name: <specify-uniq-route-name>
     namespace: <specify-namespace-where-airflow-is-installed>
   spec:
     host: <specify-your-target-host-here>
     to:
       kind: Service
       name: <airflow-api-server-service-name-for-example-airflow-webserver>
       weight: 100
     port:
       targetPort: http
     tls:
       termination: reencrypt
       destinationCACertificate: <place-CA-certificate-here-from-airflow-api-server-TLS-secret>
       insecureEdgeTerminationPolicy: Redirect
   ```

**Note**: If you can't access the apiserver host after Route creation because of "too many redirects" error, then one of the possible root
causes is, there is HTTP traffic between balancers and the cluster. To resolve that issue it is necessary to add the Route name to
the exception list at the balancers.

**Note** It might be possible to create the route in openshift automatically using annotations like `route.openshift.io/destination-ca-certificate-secret` and `route.openshift.io/termination: "reencrypt"` but this approach was not tested.

## Airflow Logging Config Classes

Airflow logging config class is a python file with logging configuration. An additional logging config class is added to the Airflow image. However, it is still possible to use the Airflow provided logging config class if required. Logging config classes can use the Airflow configuration parameters to change the overall Airflow logging behavior.

**Note**: In the default and added Airflow logging config classes, it is possible to use the `config.logging.extra_logger_names` parameter. The loggers specified in this parameter print its messages in the console with the Airflow log format. It can be set using the `config.logging.log_format` or `config.logging.colored_log_format` parameters. For example, it can be useful to add `gunicorn.access` here to have gunicorn access logs to be printed with the Airflow logging prefix. It changes messages like the following: 

```
10.105.32.131 - - [24/Aug/2021:14:50:25 +0000] "GET /health HTTP/1.1" 200 187 "-" "kube-probe/1.18"
```

to

```
[2021-08-24 14:50:25,509][glogging.py:349][INFO]10.101.32.131 - - [24/Aug/2021:14:50:25 +0000] "GET /health HTTP/1.1" 200 187 "-" "kube-probe/1.18"
```

For more information, refer to [https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#extra-loggers](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#extra-loggers).

**Note**: The gunicorn access logs can be modified by adding `--access-logformat` to apiserver arguments. 

For example:

```yaml
webserver:
...
  args: ["bash", "-c", "exec airflow api-server --access-logformat '%(t)s %(h)s %(l)s %(u)s
              \"%(r)s\" %(s)s %(b)s \"%(f)s\" \"%(a)s\"'"]
...
```

### Default Logging Config Class

With the default Airflow logging config class and with default Airflow configuration, the root logs are written to stdout. The task logs, DAG parsing, and processing logs are written to the filesystem. 
By default, with **celery executor**, `workers.persistence.enabled` is set to `true` and Airflow workers are deployed as a statefulset and require storage class for logs. Otherwise, the task logs are not available in the Airflow Web UI.
By default, with **Kubernetes executor**, task logs are stored in task worker pods and are available in the Web UI only during the task execution. Hence, to persist the logs, an additional configuration is required. The following two options are available and tested. 

To enable the default logging configuration, it is necessary to specify the following:

```yaml
airflowLocalSettings: >-
  {{- if semverCompare ">=2.2.0" .Values.airflowVersion }}

  {{- if not (or .Values.webserverSecretKey .Values.webserverSecretKeySecretName) }}

  from airflow.www.utils import UIAlert


  DASHBOARD_UIALERTS = [
    UIAlert(
      'Usage of a dynamic webserver secret key detected. We recommend a static webserver secret key instead.'
      ' See the <a href='
      '"https://airflow.apache.org/docs/helm-chart/stable/production-guide.html#webserver-secret-key">'
      'Helm Chart Production Guide</a> for more details.',
      category="warning",
      roles=["Admin"],
      html=True,
    )
  ]

  {{- end }}

  {{- end }}
config:
  logging:
    logging_config_class: ""
    task_log_reader: task
    task_log_prefix_template: ""
```

**Note**: These options are also available for Celery executor, but are not tested.

#### Using S3 Remote Storage for Storing Task Logs with Kubernetes Executor

**Note**: For S3 logging, it is necessary to ensure that the Amazon provider is added to the Airflow image.

Firstly, it is required to create an S3 bucket for Airflow logs in S3 storage.

Secondly, it is required to add an S3 connection to Airflow. For example, with name `test_s3` and with the following URI:
`aws://miniouser:miniopassword@/?endpoint_url=http%3A%2F%2Fminio.url.address%3A80&region_name=eu-central-1`.

Thirdly, it is necessary to add an S3 logging configuration in the Airflow configuration:

```yaml
config:
...
  logging:
    remote_logging: 'True'
    remote_base_log_folder: s3://source
    remote_log_conn_id: test_s3
    encrypt_s3_logs: 'False'
```

After this, the Airflow task logs are added to the /source bucket in S3 and the logs are available through the Airflow UI. 

**Note**: [delete_local_logs](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#delete-local-logs) configuration parameter can be used to remove local logs.

**Note**: For S3 connection, is possible to use https address. For additional TLS certificate validation configuration, it is possible to use extra `verify` field: it is possible to set it to `false` to ignore certificate validation, or to `/path/to/your/ca.pem` in order to use custom CA cert bundle. Here is an example of such connection in URI format: `aws://user:pwd@/?region_name=eu-central-1&endpoint_url=https%3A%2F%2Fmy.s3.addr%3A443&verify=%2Fpath%2Fto%2Fmy.cert.pem`. If needed, you can mount the custom secret with a CA cert using `extraSecrets`, `extraVolumes`, `extraVolumeMounts` parameters. It is done in the similar way as in [Keycloak With TLS](#keycloak-with-tls) , however, you will need to mount certificates both in apiserver and in workers.

#### Storing Task Logs with Kubernetes Executor in Read Write Many PVC

You can specify an external storage class for PVC and the PVC is created automatically. The storageClass in this case must support remote storage that is accessible from all K8s cluster nodes. The parameters for installation in this case are:

```yaml
logs:
  persistence:
    enabled: true
    storageClassName: nfs-external 
    size: 10Gi
```

Alternatively, it is also possible to store logs in a precreated PVC. For this, the PV must be of the readwritemany type and also must be accessible from all Airflow K8 nodes. For example, NFS/glusterfs.

Firstly, it is necessary to manually pre-create PV and PVCs with the read-write many access mode, for example:

```yaml
#PV
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: task-pv-claim
spec:
  storageClassName: externalsorageclass
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 30Gi
```

After this, the following must be set in the deployment parameters:

```yaml
logs:
  persistence:
    enabled: true
    # Volume size for logs
    size: 10Gi
    existingClaim: task-pv-claim
```

**Note**: With this, the configuration scheduler must also have access to the readwritemany PVC.

### Enabling Custom Logging Config Class

**WARNING: Writing task logs to stdout at the moment is not supported for airflow 3.**.

It is possible to enable the `QS_DEFAULT_LOGGING_CONFIG` logging config class, which comes with the qubership chart: 

```yaml
...
config:
...
  logging:
...
    logging_config_class: airflow_local_settings.QS_DEFAULT_LOGGING_CONFIG
...
airflowLocalSettings:  |-
    {{ .Files.Get "qs_files/qs_platform_logging_config.py" }}
...
```

It can be used to propagate the logs to stdout. It also allows to set the custom logging formatting for task logs.

For **celery executor**, custom logging config class allows:

* To write logs both to stdout and to filesystem. To do so, it is necessary to specify:

`workers.persistence.enabled=true`

```yaml
...
airflowLocalSettings:  |-
    {{ .Files.Get "qs_files/qs_platform_logging_config.py" }}
...
config:
...
  logging:
    qs_logging_type: stdoutfilesystem
    remote_logging: '{{- ternary "True" "False" .Values.elasticsearch.enabled }}'
    colored_console_log: 'False'
    task_log_reader: task
    audit_log_level: INFO
    logging_config_class: airflow_local_settings.QS_DEFAULT_LOGGING_CONFIG
    task_log_prefix_template: "[DAG_ID]:'{{ \"{{ ti.dag_id }}\" }}' [TASK_ID]:'{{ \"{{ ti.task_id }}\" }}' [TIMESTAMP]:'{{ \"{{ ts }}\" }}' [TRY_NUMBER]:'{{ \"{{ ti.try_number }}\" }}'"
```

* To write logs only to stdout. To do so, it is necessary to specify:

`workers.persistence.enabled=false`

```yaml
...
airflowLocalSettings:  |-
    {{ .Files.Get "qs_files/qs_platform_logging_config.py" }}
...
config:
...
  logging:
    remote_logging: '{{- ternary "True" "False" .Values.elasticsearch.enabled }}'
    colored_console_log: 'False'
    audit_log_level: INFO
    qs_logging_type: stdout
    logging_config_class: airflow_local_settings.QS_DEFAULT_LOGGING_CONFIG
    task_log_reader: stdout_task
    task_log_prefix_template: "[DAG_ID]:'{{ \"{{ ti.dag_id }}\" }}' [TASK_ID]:'{{ \"{{ ti.task_id }}\" }}' [TIMESTAMP]:'{{ \"{{ ts }}\" }}' [TRY_NUMBER]:'{{ \"{{ ti.try_number }}\" }}'"
```

For **Kubernetes executor**, it allows:

* To write logs to stdout, filesystem and to S3. To do so, it is necessary to specify:

```yaml
...
airflowLocalSettings:  |-
    {{ .Files.Get "qs_files/qs_platform_logging_config.py" }}
...
config:
...
  logging:
    remote_logging: 'True'
    remote_base_log_folder: s3://source
    remote_log_conn_id: test_s3
    encrypt_s3_logs: 'False'
    logging_config_class: airflow_local_settings.QS_DEFAULT_LOGGING_CONFIG
    qs_logging_type: stdoutfilesystem
```

**Note**: [delete_local_logs](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#delete-local-logs) configuration parameter can be used to remove local logs, so the logs will be present only in stdout and S3.

* To write logs only to stdout. To do so, it is necessary to specify:

```yaml
...
airflowLocalSettings:  |-
    {{ .Files.Get "qs_files/qs_platform_logging_config.py" }}
...
config:
...
  logging:
    remote_logging: 'False'
    remote_base_log_folder: s3://source
    remote_log_conn_id: test_s3
    encrypt_s3_logs: 'False'
    logging_config_class: airflow_local_settings.QS_DEFAULT_LOGGING_CONFIG
    qs_logging_type: stdout
    task_log_reader: stdout_task
```

**Note**: The prefix for the task logs in stdout can be specified using the `config.logging.task_log_prefix_template` parameter. These values are passed through the `tpl` function, so all are subject to being rendered as go templates. If you need to include a literal `{{` in a value, it must be expressed as follows:

```yaml
    a: '{{ "{{ not a template }}" }}'
``` 

The other prefixes that cannot be changed for other file-written logs in this case are:

* `[DAG Processing filename:/opt/airflow/dags/test_dag.py]` - For processing of custom DAG
* `[dag_processor_manager.log]` - For general DAG processing

With the `QS_DEFAULT_LOGGING_CONFIG` logging config class, it is also possible to use `config.logging.qs_processor_logging_level` and `config.logging.qs_dag_parsing_logging_level` to set logging levels for DAG processing and parsing separately.

### Audit Logs

When the `QS_DEFAULT_LOGGING_CONFIG` logging config class is used, the events related to audit are logged with the '[AUDIT]' mark.

For more information about audit logs, refer to [Airflow Audit Logs](/docs/public/audit-logs.md). Note that it is possible to configure the audit log level and format. 

For example:

```yaml
  logging:
    audit_log_level: DEBUG
    log_format_audit: '[%%(asctime)s][%%(filename)s:%%(lineno)d][%%(levelname)s][AUDIT] %%(message)s'
``` 

## Cleaning Airflow Logs

In the default and `QS_DEFAULT_LOGGING_CONFIG` logging config classes with a Celery executor, task logs are stored in the persistence storage provided by Kubernetes. For cleaning the logs in the storage, the log Groomer Sidecar container can be deployed as a part of the Airflow installation. The parameters for the sidecar can be specified under `workers.logGroomerSidecar` and `scheduler.logGroomerSidecar` in [Airflow values.yaml](/chart/helm/airflow/values.yaml). The `AIRFLOW__LOG_RETENTION_DAYS` environment variable can be used to specify the maximum age of the log files in the storage. By default, it is set to `15`.

With a Kubernetes executor, it is possible to set the policy for S3 storage to delete files in the logging bucket after the expiration time. If needed, it can be done with a python script using a custom pre-start job. The python3 script must look like the following:

```python
import boto3

def add_lificycle_policy():
    # specify connection parameters
    s3_resource = boto3.resource('s3',
                                 endpoint_url="http://minio-test.test.minio.address:80",
                                 aws_access_key_id="miniouser", aws_secret_access_key="miniopassword")
    airflow_logs_bucket = None
    for bucket in s3_resource.buckets.all():
        # specify bucket name
        if bucket.name == "airflow_logs_bucket":
            airflow_logs_bucket = bucket

    lifecycle_configuration = {
                                     'Rules': [
                                         {
                                             'Expiration': {
                                                 # specify expiration time
                                                 'Days': 1,
                                             },
                                             'Filter': {
                                                 # specify log folder name
                                                 'Prefix': 'fenrirk8s/',
                                             },
                                             'ID': 'TestOnly',
                                             'Status': 'Enabled',
                                         },
                                     ],
                                 }
    airflow_logs_bucket.LifecycleConfiguration().put(LifecycleConfiguration=lifecycle_configuration)


if __name__ == '__main__':
    add_lificycle_policy()
```

## Using Custom Pre-start Command for Pods

You can use the custom pre-start command for Airflow pods, however this command redefines the default command that starts the Airflow pod, so it is necessary to start the Airflow pod in the redefined command too. The following is an example for worker pods:

```yaml
...
workers:
...
  args:
    - "bash"
    - "-c"
    # The format below is necessary to get `helm lint` happy
    - |-
      ls -al > teststart.log; \
      exec \
      airflow {{ semverCompare ">=2.0.0" .Values.airflowVersion | ternary "celery worker" "worker" }}
...
```

**Note**: You can find the default commands that are used to start the pods in the [Qubership Airflow values.yaml](/chart/helm/airflow/values.yaml) file.

## Creating Custom Secrets

It is possible to create custom secrets using a Helm chart. In the following example, a custom secret is used to add a connection to Airflow pods:

```yaml
extraSecrets:
   '{{ .Release.Name }}-airflow-connections':
     stringData: |
       AIRFLOW_CONN_KUBERNETES_DEFAULT: 'kubernetes://?extra__kubernetes__namespace=default&extra__kubernetes__in_cluster=true'
extraEnvFrom: |
  - secretRef:
      name: '{{ .Release.Name }}-airflow-connections'
```

## Using Git Sync

Airflow deployment can use GitSync image to download DAGs from Git. 

**Note**: When using gitSync, pay attention to `dags.gitSync.maxFailures` parameter. If connection to git is unstable, setting this parameter to low value can cause frequent gitSync sideCar restarts. 

### Using Community GitSync Approach

The Airflow community helm chart provides the possibility to deploy GitSync as init and sidecar containers. For more information, see [https://airflow.apache.org/docs/helm-chart/stable/manage-dags-files.html#using-git-sync](https://airflow.apache.org/docs/helm-chart/stable/manage-dags-files.html#using-git-sync). Since the Airflow platform uses the community Airflow chart with some changes, it is possible to use a community approach for using GitSync. For example:

```yaml
extraSecrets:
  git-credentials:
    stringData: |
      GIT_SYNC_USERNAME: 'legacy'
      GIT_SYNC_PASSWORD: 'legacy'
      GITSYNC_USERNAME: 'gituser'
      GITSYNC_PASSWORD: 'gitusertokenorpassword'
dags:
  gitSync:
    wait: 5
    subPath: tests_dags_image/dags
    repo: https://github.com/Netcracker/qubership-airflow.git
    branch: main
    credentialsSecret: git-credentials
    uid: null
    enabled: true
    maxFailures: 0
    securityContexts:
      container:
        capabilities:
          drop:
            - ALL
        allowPrivilegeEscalation: false
```

If you want to use gitSync with SSH with git, refer to the following example:

```yaml
sshSecret: |-
  -----BEGIN RSA PRIVATE KEY-----

  SSH key

  content

  goes

  here

  -----END RSA PRIVATE KEY-----
extraSecrets:
  airflow-ssh-secret:
    stringData: |
      gitSshKey: '{{ .Values.sshSecret }}'
dags:
  gitSync:
    wait: 5
    subPath: tests_dags_image/dags
    repo: git@github.com:Netcracker/qubership-airflow
    branch: main
    sshKeySecret: airflow-ssh-secret
    uid: null
    enabled: true
    maxFailures: 0
    securityContexts:
      container:
        capabilities:
          drop:
            - ALL
        allowPrivilegeEscalation: false
```

**Note**: For Airflow qubership releases 2.10.5.* and above, it is possible to pass the SSH key using `dags.gitSync.sshKey` parameter instead of using `extraSecrets`.

### Using GitSync With Custom Mount Path

**Note**: This approach is not recommended and is only supported for backward compatibility.

It is possible to mount DAGs received from GitSync to custom location with the `dags.mountPath` parameter. In this case, it is also necessary to manually set the `config.core.dags_folder` parameter to the desired folder and the `dags.persistence.subPath` parameter to the folder with dags within the GitSync volume (GitSync creates a symlink with the name `repo` where all the repository content is located). For example:

```yaml
...
extraSecrets:
  git-credentials:
    stringData: |
      GIT_SYNC_USERNAME: 'legacy'
      GIT_SYNC_PASSWORD: 'legacy'
      GITSYNC_USERNAME: 'gituser'
      GITSYNC_PASSWORD: 'gitusertokenorpassword'
...
dags:
  persistence:
    subPath: repo
  mountPath: /opt/airflow/dags/custom
  gitSync:
    repo: https://github.com/Netcracker/qubership-airflow.git
    branch: main
    credentialsSecret: git-credentials
    uid: null
    enabled: true
    maxFailures: 0
    securityContexts:
      container:
        capabilities:
          drop:
            - ALL
        allowPrivilegeEscalation: false
config:
  core:
    dags_folder: /opt/airflow/dags
```

## Using Rclone to Sync DAGs From Remote Storage

The qubership platform repository includes an [Rclone](https://rclone.org/) docker image with a custom entrypoint that can be used to:

* synchronize the DAGs from remote cloud storages
* download the DAGs as a zip archive from a specific URL

The Rclone image approach works similarly to a community provided GitSync approach:

1. An Rclone init container is added to all scheduler and worker pods that downloads the DAGs from the remote storage and saves them to a volume that is used by Airflow containers as a DAG folder.
2. A sidecar container is added to all scheduler and worker pods that updates the DAGs from the remote storage in Airflow containers' DAGs volume.

As with GitSync, DAGs can be updated during the Airflow runtime.

Due to similarity, the Rclone synchronization approach uses the same configuration parameters as the GitSync approach, but the parameters have a bit different meaning. The GitSync parameters related to using Rclone instead of GitSync are described in the following table.

|Name|Description|
|---|---|
|dags.gitSync.enabled|Specifies if gitSync/Rclone is enabled.|
|dags.gitSync.rcloneImage|Specifies if rclone image should be used instead of gitSync(`false` by default).|
|dags.gitSync.maxFailures|If set to `"FAIL_ON_ERROR"`, the Rclone container will fail if there is an error downloading the DAGs from a remote source. Otherwise, it will not fail and just log an error.|
|dags.gitSync.repo|Specifies if the DAGs should be copied from the cloud storage or from a remote zip file (must be set to `urlzip` in this case). |
|dags.gitSync.rev|Specifies the URL to download the zip archive with DAGs from the cloud storage or a folder within the cloud storage.|
|dags.gitSync.period|Specifies the time period between DAGs' updating.|

**Note**: It is possible to pass the `GITSYNC_VERBOSE` environment to Rclone in order to set the Rclone logging level. 

For example:

```yaml
dags:
  gitSync:
    ...
    rcloneImage: true
    ...
    enabled: true
    ...
    env:
      - name: "GITSYNC_VERBOSE"
        value: "DEBUG"
    ...
```

### Using TLS for Rclone Endpoint

It is possible to use custom TLS certificate to connect to endpoint using Rclone. A secret with a certificate must be created in Kubernetes first. You can do it using the `extraSecrets` parameter. Alternatively, the certificate
[can be requested from cert-manager](#getting-ca-certificate-from-cert-manager)(ca.crt in the secret). To mount the secret inside the pod, you can use `*.extraVolumes` and `gitSync.extraVolumeMounts` parameters. To specify the file with the certificate inside the pod, you can use `SSL_CERT_FILE` environment variable. For example, for certificate with name `defaultsslcertificate`:

```yaml
dagProcessor:
...
  extraVolumes:
...
    - name: defaultsslcertificate
      secret:
        secretName: defaultsslcertificate
...
workers:
...
  extraVolumes:
...
    - name: defaultsslcertificate
      secret:
        secretName: defaultsslcertificate
...
dags:
  gitSync:
    env:
      - name: SSL_CERT_FILE
        value: "/ca-bundle.crt"
...
    extraVolumeMounts:
...
      - name: defaultsslcertificate
        mountPath: /ca-bundle.crt
        readOnly: true
        subPath: ca-bundle.crt
...
```

**Note:** When using the TLS enabled endpoint, it is possible to disable the certificate validation for Rclone by passing `RCLONE_NO_CHECK_CERTIFICATE` environment variable set to true, for example:
```yaml
...
dags:
  gitSync:
    env:
    - name: RCLONE_NO_CHECK_CERTIFICATE
      value: "true"
...
```

### Remote Cloud Storage

**Note**: Do not forget to add your DAGs as .py files to the cloud storage.

To synchronize the DAGs from the remote cloud storage, it is necessary to pass the remote cloud storage configuration with `[remotesrc]` config name as a secret to the Rclone container. 

It is also necessary to enable GitSync and set dags_folder to `/opt/airflow/dags`. For example, with minio config:

```yaml
...
dagProcessor:
...
  extraVolumes:
...
    - name: rclone-config
      secret:
        secretName: rclone-config
...
workers:
...
  extraVolumes:
...
    - name: rclone-config
      secret:
        secretName: rclone-config
...
rcloneConfig: |-
  [remotesrc]

  type = s3

  provider = Minio

  env_auth = false

  access_key_id = minioaccesskey

  secret_access_key = miniosecretkey

  region = eu-central-1

  endpoint = http://minio.address.qubership.com:80

  location_constraint =

  server_side_encryption =
...
extraSecrets:
...
  'rclone-config':
    stringData: |
      rclone.conf: '{{.Values.rcloneConfig}}'
...
dags:
  gitSync:
...
    rcloneImage: true
    enabled: true
    rev: airflowdags
    securityContexts:
      container:
        capabilities:
          drop:
            - ALL
        allowPrivilegeEscalation: false
    extraVolumeMounts:
      - name: rclone-config
        mountPath: /config/rclone/rclone.conf
        readOnly: true
        subPath: rclone.conf
...
config:
...
  core:
...
    dags_folder: /opt/airflow/dags
...
```

### Using Downloadable ZIP Archive with DAGs

To download DAGs' ZIP archive from URL, it is necessary to set `dags.gitSync.repo` to `urlzip` and `dags.gitsync.rev` to the required URL. It is also necessary to set dags_folder to `/opt/airflow/dags`. 

For example:

```yaml
dags:
  gitSync:
    rcloneImage: true
    repo: urlzip
    rev: https://remote.http.server/dr_dag.zip
    uid: null
    enabled: true
    securityContexts:
      container:
        capabilities:
          drop:
            - ALL
        allowPrivilegeEscalation: false
...
config:
...  
  core:
...    
    dags_folder: /opt/airflow/dags
...
```

## Kerberos Support and HostAliases for Workers

**Note**: Kerberos support is only available for Kubernetes executor.

To enable Kerberos support the following parameters are required:
* `config.core.security`—This parameter must be set to `kerberos` to enable the Kerberos support in Airflow.
* `workers.kerberosSidecar.enabled`—This parameter must be set to `true` to enable Kerberos sidecar in worker pods that updates Kerberos tickets.
* `kerberos.enabled`—This parameter must be set to `true` to enable Kerberos configurations.
* `kerberos.ccacheMountPath`—This parameter specifies the location of the ccache volume. By default it is set to `/var/kerberos-ccache`.
* `kerberos.ccacheFileName`—This parameter specifies the name of the ccache file. By default it is set to `ccache`.
* `kerberos.configPath`—This parameter specifies the path for the Kerberos configuration file. By default it is set to `/etc/krb5.conf`.
* `kerberos.keytabPath`—This parameter specifies the path for the Kerberos keytab file. By default it is set to `/etc/airflow.keytab`.
* `kerberos.principal`—This parameter specifies the name of the Kerberos principal. By default it is set to `airflow`.
* `kerberos.reinitFrequency`—This parameter specifies the frequency of reinitialization of the Kerberos token. By default is set to `3600`.
* `kerberos.config`—This parameters specifies the content of the configuration file for Kerberos. For more information, see [Airflow values.yaml](/charts/helm/airflow/values.yaml) for the default value.
* `kerberos.keytabBase64Content`—This parameter can be used to specify the keytab file content encoded in base64 format. If not specified, it is necessary to pre-create the secret in the namespace using the following command:
   `kubectl create secret generic {{ .Release.name }}-kerberos-keytab --from-file=kerberos.keytab`.
   Where,
    `{{ .Release.name }}` is `airflow-namespace_name` and the keytab file must be named `kerberos.keytab`.

The following is an example of Kerberos parameters:

```yaml
...
kerberos:
  enabled: true
  keytabBase64Content: BASE64ENCODEDKEYTAB
  ccacheMountPath: '/var/kerberos-ccache'
  ccacheFileName: 'cache'
  configPath: '/etc/krb5.conf'
  keytabPath: '/etc/airflow.keytab'
  principal: 'airflow-user@LDAP.REALM'
  reinitFrequency: 3600
  config: |
    # This is an example config showing how you can use templating and how "example" config
    # might look like. To make it production-ready you must replace it with your own configuration that
    # Matches your kerberos deployment. Administrators of your Kerberos instance should
    # provide the right configuration.
    [libdefaults]
    #renew_lifetime = 7d
    forwardable = true
    default_realm = TESTAD.LOCAL
    ticket_lifetime = 24h
    dns_lookup_realm = false
    dns_lookup_kdc = false
    default_ccache_name = /tmp/krb5cc_%{uid}
    #default_tgs_enctypes = aes des3-cbc-sha1 rc4 des-cbc-md5
    #default_tkt_enctypes = aes des3-cbc-sha1 rc4 des-cbc-md5
    [logging]
    default = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmind.log
    kdc = FILE:/var/log/krb5kdc.log
    [realms]
    TESTAD.LOCAL = {
    admin_server = DC.ldap.realm
    kdc = DC.ldap.realm
    }
    QUBERSHIP.COM = {
    }
    [domain_realm]
    ldap.realm = LDAP.REALM
    .ldap.realm = LDAP.REALM
...
workers:
...
  kerberosSidecar:
    enabled: true
...
# if needed, add hostAliases:
  hostAliases:
    - ip: "10.118.32.211"
      hostnames:
      - "hadoop1.node.com"
    - ip: "10.118.32.212"
      hostnames:
      - "hadoop2.node.com"
...
config:
  core:
    security: 'kerberos'
...
```

**Note**: The Airflow service does not support an update from non-Kerberos installation to Kerberos and vice versa. If required, it is possible to reinstall Airflow service.

**Keytab File Generation**

The following is an example of keytab file generation in the environment where Kerberos is configured and `ktutil` command is available:

```
ktutil
ktutil: add_entry -password -p $AIRFLOW_USER -k 1 -e aes256-cts-hmac-sha1-96
ktutil: wkt $KEYTAB_FILENAME
ktutil: exit
```

Where,

* `$AIRFLOW_USER` - Airflow user to work with Kerberos.
* `$KEYTAB_FILENAME` - The name of keytab file that is created locally, `airflow.keytab`.

To get base64 keytab file content, you can use the `base64 $KEYTAB_FILENAME` command.

## LDAP Support for Web UI

**Note:** This section assumes that FAB auth manager is used. Airflow FAB provider is present in the image by default. To enable FAB auth manager in the config, it is necessary to set:

```yaml
config:
  core:
    auth_manager: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

You can enable LDAP integration for Web UI using the installation parameters. For more information, refer to [https://airflow.apache.org/docs/apache-airflow/3.0.2/security/webserver.html](https://airflow.apache.org/docs/apache-airflow/3.0.2/security/webserver.html), [https://flask-appbuilder.readthedocs.io/en/latest/security.html](https://flask-appbuilder.readthedocs.io/en/latest/security.html), and [https://flask-appbuilder.readthedocs.io/en/latest/config.html](https://flask-appbuilder.readthedocs.io/en/latest/config.html). The `webserver_config.py` can be specified using the `apiServer.apiServerConfig` parameter.

The following is an example for enabling LDAP without group mapping and with pre-created Admin user. 

```yaml
apiServer:
...
  defaultUser:
    enabled: true
    role: Admin
    username: ldap_admin_username
...
  apiServerConfig: |
    import os
    from airflow.configuration import conf
    from flask_appbuilder.security.manager import AUTH_LDAP
    basedir = os.path.abspath(os.path.dirname(__file__))
    SQLALCHEMY_DATABASE_URI = conf.get('database', 'SQL_ALCHEMY_CONN')
    CSRF_ENABLED = True
    AUTH_TYPE = AUTH_LDAP
    AUTH_ROLE_ADMIN = 'Admin'
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = 'Viewer'
    AUTH_LDAP_SERVER = 'ldap://openldap.qubership.com:18224'
    AUTH_LDAP_SEARCH = 'ou=users,ou=quber,dc=qubership,dc=com'
    AUTH_LDAP_BIND_USER = 'cn=Manager,dc=qubership,dc=com'
    AUTH_LDAP_BIND_PASSWORD = 'ldapbindpassword'
    AUTH_LDAP_UID_FIELD = 'cn'
    # LDAPS
    AUTH_LDAP_USE_TLS = False
...
```

In the example above, the `webserver.defaultUser.*` are required for providing `Admin` role to the `ldap_admin_username` user. This user must also be present in LDAP for successful login. 

The following is an example for enabling LDAP with group mapping:

```yaml
webserver:
...
  defaultUser:
    enabled: false
apiServer:
...
  apiServerConfig: |
    import os
    from airflow import configuration as conf
    from flask_appbuilder.security.manager import AUTH_LDAP
    basedir = os.path.abspath(os.path.dirname(__file__))
    SQLALCHEMY_DATABASE_URI = conf.get('database', 'SQL_ALCHEMY_CONN')
    CSRF_ENABLED = True
    AUTH_TYPE = 2
    AUTH_LDAP_SERVER = 'ldap://openldap.qubership.com:18224'
    AUTH_LDAP_SEARCH = 'ou=quberqa,dc=qubership,dc=com'
    AUTH_LDAP_BIND_USER = 'cn=Manager,dc=qubership,dc=com'
    AUTH_LDAP_BIND_PASSWORD = 'ldapbindpassword'
    AUTH_LDAP_UID_FIELD = 'cn'
    AUTH_LDAP_GROUP_FIELD = "memberOf"
    AUTH_USER_REGISTRATION = True
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_ROLES_MAPPING = {
    "cn=my_test_group_1,ou=groups,ou=quberqa,dc=qubership,dc=com":
    ["Admin"],
    "cn=my_test_group_2,ou=groups,ou=quberqa,dc=qubership,dc=com":
    ["Viewer"]
    }
    # LDAPS
    AUTH_LDAP_USE_TLS = False
```

**Note**: If required, it is possible to store the `AUTH_LDAP_BIND_PASSWORD` in a secret created using the `extraSecrets` parameter. Load it into the pod using the `extraEnvFrom` parameter and use `os.getenv` in `apiServerConfig`. For example:

```yaml
extraSecrets:
   'ldap-bind-password':
     stringData: |
       AUTH_LDAP_BIND_PASSWORD: 'ldapbindpassword'
...
extraEnvFrom: |
  - secretRef:
      name: 'ldap-bind-password'
...
webserver:
...
  defaultUser:
    enabled: false
apiServer:
...
  apiServerConfig: |
    import os
    from airflow import configuration as conf
    from flask_appbuilder.security.manager import AUTH_LDAP
    basedir = os.path.abspath(os.path.dirname(__file__))
    SQLALCHEMY_DATABASE_URI = conf.get('database', 'SQL_ALCHEMY_CONN')
    CSRF_ENABLED = True
    AUTH_TYPE = 2
    AUTH_LDAP_SERVER = 'ldap://openldap.qubership.com:18224'
    AUTH_LDAP_SEARCH = 'ou=quberqa,dc=qubership,dc=com'
    AUTH_LDAP_BIND_USER = 'cn=Manager,dc=qubership,dc=com'
    AUTH_LDAP_BIND_PASSWORD = os.getenv('AUTH_LDAP_BIND_PASSWORD')
    AUTH_LDAP_UID_FIELD = 'cn'
    AUTH_LDAP_GROUP_FIELD = "memberOf"
    AUTH_USER_REGISTRATION = True
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_ROLES_MAPPING = {
    "cn=my_test_group_1,ou=groups,ou=quberqa,dc=qubership,dc=com":
    ["Admin"],
    "cn=my_test_group_2,ou=groups,ou=quberqa,dc=qubership,dc=com":
    ["Viewer"]
    }
    # LDAPS
    AUTH_LDAP_USE_TLS = False
```

**Note**: Currently, the flower pod does not support LDAP integration.

## Keycloak Web UI Integration

**Note:** This section assumes that FAB auth manager is used. Airflow FAB provider is present in the image by default. To enable FAB auth manager in the config, it is necessary to set:

```yaml
config:
  core:
    auth_manager: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

As with LDAP, it is possible to enable Keycloak Web UI integration for Web UI using the installation parameters. For more information, refer to [https://airflow.apache.org/docs/apache-airflow/2.10.5/security/webserver.html](https://airflow.apache.org/docs/apache-airflow/2.10.5/security/webserver.html), [https://flask-appbuilder.readthedocs.io/en/latest/security.html](https://flask-appbuilder.readthedocs.io/en/latest/security.html), and [https://flask-appbuilder.readthedocs.io/en/latest/config.html](https://flask-appbuilder.readthedocs.io/en/latest/config.html). The `webserver_config.py` can be specified using the `apiServer.apiServerConfig` parameter. 

The qubership chart distribution includes a [webserver_config.py](/chart/helm/airflow/qs_files/webserver_config_keycloak.py) example file that can be used for the integration with IDP.

This file can be loaded in the `apiServer.apiServerConfig` parameter, for example:

```yaml
apiServer:
  defaultUser:
    enabled: false
  apiServerConfig: |-
    {{ .Files.Get "qs_files/webserver_config_keycloak.py" }}
...
```

For this file to work, a client must be created in Keycloak with a redirect URI set to the Airflow Web UI ingress.

Also, it is necessary to specify the following environment variables for Airflow pods:

* `KEYCLOAK_REALM_ID`- To specify the Keycloak realm ID that was created using the IDP cloud administrator panel.
* `CLIENT_ID`- Name of the UI client in Keycloak to use.
* `PRIVATE_GATEWAY_EXTERNAL_URL`- External URL for keycloak(ingress). For example, `https://ingress.keycloak.qubership.com`.
* `PRIVATE_GATEWAY_INTERNAL_URL`- Internal URL for keycloak(service). For example, `http://keycloak.keycloak-namespace.svc:8080`.
* `AIRFLOW_KEYCLOAK_ADMIN_ROLES`- A comma separated list of roles in Keycloak that is mapped to the Airflow admin role. If the user in Keycloak does not have this role, the users in Airflow will have a `viewer` role.
* `APP_URL_LOGOUT`- Optional variable. Can be used to specify where the user will be redirected after logout from airflow. By default, the user will be redirected to airflow login page.

### Keycloak With TLS

It is possible to use an https address for `PRIVATE_GATEWAY_EXTERNAL_URL`. If the custom certificate (including the ones from cert-manager) or self-signed certificate are used, it is possible to mount these certificates into the pod and to set `REQUESTS_CA_BUNDLE` and `SSL_CERT_FILE` environment varibles to point to this file, for example:

```yaml
extraEnv: >-
  - name: REQUESTS_CA_BUNDLE
    value: /opt/airflow/mysslcert.crt
  - name: SSL_CERT_FILE
    value: /opt/airflow/mysslcert.crt
extraSecrets:
  mysslcert:
    stringData: >
      mysslcert.crt: -----BEGIN CERTIFICATE-----

                     certificate

...
                     content

                     goes here

                     -----END CERTIFICATE-----
apiServer:
  extraVolumeMounts:
    - name: mysslcert
      mountPath: /opt/airflow/mysslcert.crt
      subPath: mysslcert.crt
      readOnly: true
  extraVolumes:
    - name: mysslcert
      secret:
        secretName: mysslcert
```
If needed, as CA certificate it is possible to use pre-created secret with certificate, `defaultsslcertificate`(ca-bundle.crt in the secret) or [CA Certificate From Cert-Manager](#getting-ca-certificate-from-cert-manager)(ca.crt in the secret). 

## Enabling HTTPS for Airflow Ingresses

Some ingress providers allow setting default certificates and forcing HTTPS for all ingresses in a cluster. For example, refer to nginx ingress details at [https://kubernetes.github.io/ingress-nginx/user-guide/tls/#default-ssl-certificate](https://kubernetes.github.io/ingress-nginx/user-guide/tls/#default-ssl-certificate). In this case, all ingresses in the k8s cluster are using the same default certificate.

However, it is possible to use TLS for Airflow ingresses (API/flower) separately using a custom certificate. For example, for API server ingress:

```yaml
ingress:
  enabled: true
  apiServer:
    tls:
      enabled: true
      secretName: ingresscerts
    hosts:
      - web-airflow.kubernetes.qubership.com
```

In this example, `ingresscerts` is a name of the secret with certificates. It can be created manually before the installation. It also can be created during the installation using the `extraSecrets` parameter, for example:

```yaml
extraSecrets:
  'ingresscerts':
    type: kubernetes.io/tls
    stringData: |
      tls.crt: -----BEGIN CERTIFICATE-----

                certificate

                goes

...

                here

                -----END CERTIFICATE-----
      tls.key: -----BEGIN RSA PRIVATE KEY-----

                 RSA private key

                 goes
...

                 here

                 -----END RSA PRIVATE KEY-----
```

To use the default Kubernetes secret, do not specify `ingress.apiServer.secretName`.

### Using Cert-manager to Get Certificate for Ingress

**Note**: [cert-manager](https://cert-manager.io/) must be installed in the cluster for this to work. 

Cert-manager has support for providing https for ingresses by providing additional annotations. For more information, see [https://cert-manager.io/docs/usage/ingress/](https://cert-manager.io/docs/usage/ingress/). Airflow chart allows adding custom annotations for Airflow ingress and therefore uses cert-manager for securing Airflow ingress resources. For example:

```yaml
ingress:
  enabled: true
  apiServer:
    annotations:
      cert-manager.io/cluster-issuer: qa-issuer-self
    tls:
      enabled: true
      secretName: myingress-cert
    hosts:
      - web-airflow.kubernetes.qubership.com
```

## Enabling HPAs for workers and webserver

It is possible to configure HPAs for workers and webservers using `webserver.hpa.*` and `workers.hpa.*` parameters, for example:

```yaml
workers:
  hpa:
    behavior:
      scaleUp:
        stabilizationWindowSeconds: 600
        policies:
          - type: Percent
            value: 100
            periodSeconds: 15
          - type: Pods
            value: 1
            periodSeconds: 15
        selectPolicy: Max
    enabled: true
    minReplicaCount: 1
    maxReplicaCount: 3
    metrics:
      - type: Resource
        resource:
          name: cpu
          target:
            type: Utilization
            averageUtilization: 50
```

For more information, please refer to the _Kubernetes Official Documentation_ at [https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) .

## Prometheus Monitoring and Alerts

This section provides details about Prometheus monitoring and alerts.

### Plugin Prometheus Monitoring

The platform image contains the airflow-exporter plugin that exposes Prometheus metrics in the Web UI pod at the `/admin/metrics` endpoint. For more information about airflow-exporter, refer to [https://github.com/epoch8/airflow-exporter](https://github.com/epoch8/airflow-exporter). It allows using Grafana Dashboard and Prometheus alerts. 

**Note**: It requires Prometheus Operator with sufficient permissions installed in the cluster.

To enable service monitor that gathers Prometheus metrics, you must specify the following in the installation parameters:

```yaml
serviceMonitor:
  enabled: true
  interval: "30s"
```

**Note**: It is also possible to specify [Prometheus `scrapeTimeout`](https://github.com/prometheus-operator/prometheus-operator/blob/main/Documentation/api.md#endpoint), for example:

```yaml
serviceMonitor:
  enabled: true
  interval: "180s"
  scrapeTimeout: "150s"
```

However, you should avoid long Prometheus scrapes, as it is better to increase the Web UI and Postgres resources.

### StatsD Prometheus Exporter Monitoring

The chart also allows to use [official airflow metrics](https://airflow.apache.org/docs/apache-airflow/3.0.2/logging-monitoring/metrics.html) with [statsd_exporter](https://github.com/prometheus/statsd_exporter). To enable StatsD exporter and service monitor that gathers Prometheus metrics from the StatsD exporter, you must specify the following in the installation parameters:

```yaml
statsd:
  enabled: true
...
serviceMonitorStatsd:
  enabled: true
```

**Note**: It is possible to change the logging level of the statsd pod by adding `--log.level=debug` to statsd arguments. For example:

```yaml
statsd:
...
  args: ["--statsd.mapping-config=/etc/statsd-exporter/mappings.yml", "--log.level=debug"]
...
```

### Grafana Dashboard

Qubership platform Grafana Dashboard is installed when the `platformAirflowMonitoring` parameter is set to `true`:
 
```yaml
platformAirflowMonitoring: true
```

The dashboard has panels that use airflow-exporter plugin and that use StatsD Prometheus exporter metrics. For more information about dashboard, refer to _[Cloud Platform Monitoring Guide](/docs/public/monitoring.md)_.

### Prometheus Alerts

To enable Prometheus alerts, you must specify the following in the installation parameters:

```yaml
prometheusRule:
  enabled: true
  additionalLabels:
    app.kubernetes.io/component: monitoring
```

To enable Kubernetes alerts, based on Kubernetes metrics, you must set the `prometheusRule.kubeAlerts` parameter to `true`. For example:

```yaml
prometheusRule:
  enabled: true
  kubeAlerts: true
  additionalLabels:
    app.kubernetes.io/component: monitoring
```

It is also possible to enable the alerts for failed DAGs/tasks or long running DAGs. For example,

```yaml
platformAirflowMonitoring: true
serviceMonitor:
  enabled: true
  selector:
    app.kubernetes.io/component: monitoring
  path: /admin/metrics
  interval: "30s"
prometheusRule:
  additionalLabels:
    app.kubernetes.io/component: monitoring
  enabled: true
  dagAlerts:
    enabled: true
    taskFailed: true
    maxDagRunDuration: 3600
```

For more information about alerts, refer to _[Cloud Platform Monitoring Guide](/docs/public/monitoring.md)_.

You can also specify custom alerts. For example, you can specify the alert for log leaning dag running too long:

```yaml
prometheusRule:
  enabled: true
  kubeAlerts: true
  additionalLabels:
    app.kubernetes.io/component: monitoring
  groups: [
  {
    "name": "general.rules",
    "rules": [
      {
        "alert": "Cleaner dag is stuck",
        "annotations": {
          "description": "Cleaner dag is stuck",
          "summary": "Cleaner dag is stuck"
        },
        "expr": "max(airflow_dag_run_duration{namespace=\"airflow2\", dag_id=\"airflow-pod-log-cleanup\"}) > 3600",
        "for": "1m",
        "labels": {
          "severity": "High",
          "namespace": "airflow",
          "service": "airflow"
        }
      }
    ]
  }
]
```

## Status Provisioner Job

**Note**: To disable the job, set `statusProvisioner.enabled` to "false".

Status Provisioner is a component for providing the overall service status.

|Name|Description|
|---|---|
|statusProvisioner.enabled|Specifies if the status provisioner job is deployed.|
|statusProvisioner.dockerImage|The image for the Deployment Status Provisioner pod.|
|statusProvisioner.lifetimeAfterCompletion|The number of seconds that the job remains alive after its completion.|
|statusProvisioner.podReadinessTimeout|The timeout in seconds that the job waits for the monitored resources to be ready or completed.|
|statusProvisioner.integrationTestsTimeout|The timeout in seconds that the job waits for the integration tests to complete.|
|statusProvisioner.labels|Labels to add to Status Provisioner objects.|
|statusProvisioner.securityContext|The pod-level security attributes and common container settings for the Status Provisioner pod.|
|statusProvisioner.resources|The pod resource requests and limits.|
|statusProvisioner.priorityClassName|Specifies the priority class name for Status Provisioner.|

```yaml
statusProvisioner:
  enabled: true
  dockerImage: ghcr.io/netcracker/qubership-deployment-status-provisioner:main
  lifetimeAfterCompletion: 600
  podReadinessTimeout: 300
  integrationTestsTimeout: 300
  labels: {}
  securityContexts:
    pod: {}
    container: {}
  resources: {}
```

**Note**: In case of installation in the DR schema, the Status Provisioner skips status checking for components that have replicas set to 0.

## Integration Tests

**Note**: Airflow integration tests require API authentication to be enabled. During the Airflow installation with tests, the following parameter should be specified:

```
config:
  ...
  api:
    auth_backend: airflow.providers.fab.auth_manager.api.auth.backend.basic_auth
  ...
```

|Name|Description|
|---|---|
|integrationTests.enabled|Specifies if the integration tests' components are deployed.|
|integrationTests.service.name|Specifies the name of Airflow integration tests' service.|
|integrationTests.secret.airflow.user|Specifies the user for authentication in Airflow.|
|integrationTests.secret.airflow.password|Specifies the password for authentication in Airflow.|
|integrationTests.serviceAccount.create|Specifies whether the service account for Airflow integration tests is to be deployed or not.|
|integrationTests.serviceAccount.name|Specifies the name of the service account that is used to deploy Airflow integration tests.|
|integrationTests.image|Specifies the Docker image of Airflow integration tests.|
|integrationTests.tags|Specifies the tags combined together with `AND`, `OR`, and `NOT` operators that select test cases to run.|
|integrationTests.airflowHost|Specifies the host name of the Airflow API server.|
|integrationTests.airflowPort|Specifies the port of Airflow.|
|integrationTests.workerServiceName|Specifies the name of the Airflow Worker service.|
|integrationTests.apiServiceName|Specifies the name of the Airflow API service.|
|integrationTests.schedulerDeployment|Specifies the name of the Airflow Scheduler deployment.|
|integrationTests.prometheusHost|Specifies the host name of the Prometheus service.|
|integrationTests.prometheusPort|Specifies the port of the Prometheus service.|
|integrationTests.executorType|Specifies the type of worker executor. The possible value is `CeleryExecutor` or `KubernetesExecutor`.|
|integrationTests.securityContexts|Specifies the pod security context for the Airflow integration tests' pod.|
|integrationTests.statusWritingEnabled|Specifies the writing status of integration tests' execution to the specified Custom Resource.|
|integrationTests.priorityClassName|Specifies the priority class name for integration tests.|

```yaml
integrationTests:
  enabled: false
  service:
    name: airflow-integration-tests
  secret:
    airflow:
      user: "admin"
      password: "admin"
  serviceAccount:
    create: true
    name: "airflow-integration-tests"
  image: "ghcr.io/netcracker/qubership-airflow-integration-tests:main"
  tags: "smoke"
  airflowHost: "airflow-api-server"
  airflowPort: 8080
  workerServiceName: "airflow-worker"
  apiServiceName: "airflow-api-server"
  schedulerDeployment: "airflow-scheduler"
  prometheusHost: ""
  prometheusPort: 9090
  executorType: "CeleryExecutor"
  statusWritingEnabled: "true"
  securityContexts:
    pod: {}
    container: {}
  resources: {}
```

### Tags Description

This section contains information about integration test tags that can be used to test the Airflow service. You can use the following tags:

* `airflow` tag runs all tests connected to the Airflow service.
    * `smoke` tag runs all tests connected to the smoke scenarios like `Check Status API`, `Run Dag To Check PG Connection`, and `Run noop_dag` tests.
      * `pg_connection_dag` tag runs `Run Dag To Check PG Connection` test.
    * `ha` tag runs all tests connected to the HA scenarios.
      * `worker` tag runs `Test HA Case With Worker Pods Sleep Dag` and `Test HA Case With Worker Pods Sleep Dag With Retries` tests.
      * `scheduler` tag runs `Test HA Case With Scheduler Pod Sleep Dag With Retries` test.
      * `api` tag runs `Test HA Case With API pod` test.
    * `alert` tag runs all tests connected to Monitoring scenarios.
      * `worker_degraded` tag runs `Test Alert Worker Statefulset/Deployment Is Degraded` test.
      * `worker_down` tag runs `Test Alert Worker Error` test.
      * `worker_down_firing` tag runs `Test Alert Worker Error With Firing State` test.
      * `scheduler_down` tag runs `Test Alert Scheduler error` test.
      * `api_down` tag runs `Test Alert API error` test.
    * `airflow_images` tag runs all tests connected to check pod images' scenarios.
      * `airflow_images_worker` tag runs `Test Hardcoded Images In Worker` test.
      * `airflow_images_scheduler` tag runs `Test Hardcoded Images In Scheduler` test.
      * `airflow_images_apiserver` tag runs `Test Hardcoded Images In Apiserver` test.
      * `airflow_images_tests` tag runs `Test Hardcoded Images In Tests` test.
      * `airflow_images_statsd` tag runs `Test Hardcoded Images In Statsd` test.
    
**Note**: In case of running tests with the `airflow` tag, increase the timeouts for the status provisioner `statusProvisioner.integrationTestsTimeout: 3000`.

**Note**: For proper work of all integration tests, the following dags should be present in Airflow: `noop_dag`, `sleeping_dag`, `sleep_dag_with_retries`, `postgres_operator_test_dag`.
    
# Installation

For more information about Helm parameters, refer to the _Official Airflow Helm Repository_ at [https://github.com/apache/airflow/tree/main/chart](https://github.com/apache/airflow/tree/main/chart).


**Note**: When deploying with ArgoCD or ArgoCD based tools, it is necessary to add `"argocd.argoproj.io/hook": "Sync"`, 
`"argocd.argoproj.io/hook-delete-policy": "HookSucceeded"` annotations to the Airflow Migrate Database Job, and `"argocd.argoproj.io/hook": "PostSync"`, 
`"argocd.argoproj.io/hook-delete-policy": "HookSucceeded"` annotations to the Airflow Create User Job. It is possible to do using `.Values.createUserJob.jobAnnotations` and `.Values.migrateDatabaseJob.jobAnnotations` parameters. Also, it might be necessary to set the `useHelmHooks` parameter to `false`, for example:

```yaml
...
  migrateDatabaseJob:
    useHelmHooks: false
    jobAnnotations:
      argocd.argoproj.io/hook: "Sync"
      argocd.argoproj.io/hook-delete-policy: "HookSucceeded"
...
  createUserJob:
    jobAnnotations:
      argocd.argoproj.io/hook: "PostSync"
      argocd.argoproj.io/hook-delete-policy: "HookSucceeded"
...
```

**Note**: Unless the `config.core.dags_are_paused_at_creation` parameter is set to `false` or the DAGs are specifically created with the `is_paused_upon_creation` parameter, Airflow DAGs are disabled by default and must be enabled manually in the Web UI if required. For more information, refer to [https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#dags-are-paused-at-creation](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#dags-are-paused-at-creation) and [https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#module-airflow.models.dag](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#module-airflow.models.dag).

## On-Prem

The information for On-Prem is specified below.

### HA Scheme

For HA scheme, it is necessary to have more than one scheduler and worker (for Celery executor). If Web UI is important in your case, it is possible to have more than one Web UI too. 
The parameters responsible for it are:
`scheduler.replicas`,`workers.replicas`, `dagProcessor.replicas` and `apiServer.replicas`.

For providing high availability in case of accessing task logs, refer to the [Airflow Logging Config Classes](#airflow-logging-config-classes) section.

### DR Scheme

For DR scheme, refer to the [Airflow Site Manager and DR Deployment](#airflow-site-manager-and-dr-deployment) section.

### Non-HA Scheme

For the non-HA scheme, it is possible to set each component number to one. In case of accessing logs, it is possible to write the task logs only to stdout.

## AWS

In AWS Kubernetes, Airflow can be installed similar to common Kubernetes. SSL ElasticCache is supported, and Redis connection in this case can be specified as follows:

```yaml
...
data:
...
  brokerUrl: redis://airflowredis.your.elasticcache.address
...
```

You can also refer to Amazon MWAA in AWS at [https://docs.aws.amazon.com/mwaa/latest/userguide/what-is-mwaa.html](https://docs.aws.amazon.com/mwaa/latest/userguide/what-is-mwaa.html).

**Note**: For more extensive configuration and easier troubleshooting, it is recommended to use the Qubership Platform Airflow with Qubership Platform Redis.

## Manual Helm Installation

For the Airflow installation, preinstall Helm 3 referring to [https://github.com/helm/helm/releases/tag/v3.0.0](https://github.com/helm/helm/releases/tag/v3.0.0). Download the helm chart from the [chart/helm/airflow](/chart/helm/airflow) folder and configure the [values.yaml](/chart/helm/airflow/values.yaml) file. 

After you configure the [values.yaml](/chart/helm/airflow/values.yaml) file parameters, you can deploy Airflow using the following command:

```
$ helm install airflow <path_to_helm_chart_folder> --namespace <airflow_namespace> --kubeconfig <path_to_kubeconfig> 
```

**Note**: The images for the **values.yaml** file can been found in packages or on release page.

# Upgrade

Airflow supports the helm update. Therefore, the app and deployer jobs also support the update and the process is similar to installation. Note that `data.brokerUrl` and `fernetKey` cannot be changed during the upgrade.

**Note**: In the custom image, it is also necessary to update the DAGs and ensure that the required providers are installed. For more information, refer to the _Official Airflow Documentation_ at [https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-2.html#step-5-upgrade-airflow-dags](https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-2.html#step-5-upgrade-airflow-dags) and [https://airflow.apache.org/docs/apache-airflow-providers/#community-maintained-providers](https://airflow.apache.org/docs/apache-airflow-providers/#community-maintained-providers).

**Note**: If a new Airflow version modifies the PG Airflow database, it is not possible to rollback from the new Airflow version (unless you have a PG backup with the previous version).
