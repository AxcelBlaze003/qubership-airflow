[[_TOC_]]

## Repository structure

* `.github/workflows` - folder related to Qubership platform Airflow pipeline config.
* `chart` - Qubership platform helm charts for Airflow. Also includes custom python logging configuration and python IDP integration configuration(used as part of the chart).
* `docker` - dockerfile for Qubership platform airflow docker image and related files. This includes python scripts for database creation(manual and with DBAAS). Also includes DBAAS integration package.
* `docs` - Qubership platform airflow documentation.
* `integration-tests` - files related to Qubership platform airflow integration tests.
* `rclone-image` - dockerfile and related filed for rclone image used in some Qubership platform airflow deployments.
* `site-manager` - files related to Qubership platform airflow site manager
* `tests_dags_image` - test airflow DAGs and airflow image that includes these DAGs and necessary python libraries to run these DAGs.

## How to start

Airflow Qubership platform distribution is largely based on airflow official slim image and includes airflow community helm chart with minor changes and some additions. Airflow Qubership platform distribution also includes rclone/gitsync images for syncing DAGs, site manager, status provisioner that is based on Qubership platform common status provisioner, tests image that is based on Qubership platform common tests image.

### Useful links:
Official airflow docs:

* https://airflow.apache.org/docs/ - general airflow documentation
* https://airflow.apache.org/docs/docker-stack/index.html - community airflow docker images information
* https://airflow.apache.org/docs/helm-chart/stable/index.html - airflow helm chart documentation
* https://airflow.apache.org/docs/helm-chart/stable/production-guide.html - production guide for airflow helm chart
* https://github.com/apache/airflow/tree/main - airflow repository
* https://github.com/apache/airflow/tree/main/chart - airflow community helm chart location
* https://github.com/apache/airflow/tree/main/providers/src/airflow/providers - airflow providers location
* https://github.com/apache/airflow/blob/main/airflow/config_templates/airflow_local_settings.py - airflow default logging config

Rclone and gitSync:

* https://github.com/rclone/rclone - official rclone github repository
* https://rclone.org/ - official rclone docs.
* https://github.com/kubernetes/git-sync - official gitsync github repository.

Internal docs of services that airflow uses or has integration with:

* https://github.com/Netcracker/qubership-dbaas/tree/main/docs - Qubership DBAAS docs (mostly useful for airflow is rest api).
* https://github.com/Netcracker/qubership-maas/ - Qubership MAAS repository.
* https://www.keycloak.org/documentation - Keycloak dicumentation
* https://github.com/Netcracker/qubership-deployment-status-provisioner - Qubership status provisioner
* https://github.com/Netcracker/qubership-docker-integration-tests - Qubership integration tests image
* https://github.com/Netcracker/qubership-disaster-recovery-daemon - qubership disaster recovery daemon, used as a GO lirbrary in qubership airflow site manager
* https://github.com/Netcracker/DRNavigator - Qubership site manager

### Build

Qubership platform airflow can deploy the following images:

* Qubership platform airflow base image - this image is based on community airflow slim image and needs to be built.
* Airflow site manager image - this image contains airflow logic to work with qubership site manager. This image needs to be built.
* Rclone image - this image is based on community rclone image and includes some additional logic to work similarly to gitSync in airflow deployment. This image needs to be built.
* Integration tests image - this image contains integration tests for airflow and is based on qubership integration tests image.
* Gitsync image - gitsync community image. Community image is reused, build is not needed.
* Statsd Prometheus exporter - StatSD prometheus community image used for monitoring. Community image is reused, no build is needed.
* Status provisioner image - image that is used to report status for deployer. Qubership status provisioner is reused, build is not needed.

Build pipeline logic is implemented at [.github/workflows](/.github/workflows) folder.

#### Definition of done

The changes can be merged to main if:

1. Ticket task is done.
2. All the pipeline steps can be built.
3. The changes were tested by QA.

### Deploy to k8s

See [installation.md](/docs/public/installation.md)

## How to debug and troubleshoot

For general configuration troubleshooting, refer to [troubleshooting.md](/docs/public/troubleshooting.md)

Most common issues and respective most common place where to look for a problem:

* Problems with `helm install` command during deployment with errors in helm chart files: check helm chart.
* Problems with `helm install` command with it being stuck: check database configuration issues, check for jobs in k8s namespace. Most probably you need to check  [DBAAS configuration related issues](#dbaas-configuration-related-issues).
* Problems with status provisioner after successful `helm install` command: check problematic pods and containers and act accordingly
* Problems with airflow login with IDP integration: check [IDP Configuration related issues](#idp-configuration-related-issues)
* Problems with main airflow pods(including jobs) not being able to start: check error. If the error is related to logging, check [Logging Config Issues](#logging-config-issues)
* Problems with airflow pod restarts: Check log errors. Check [Performance issues](#performance-issues). If the error is related to gitSync container, check [GitSync container issues](#gitsync-container-issues)

### Performance issues

When investigating performance issues firstly it is necessary to ensure that PG/redis have enoug resources and connection slots available. This can be done using PG/redis or general monitoring.

Next thing to check is if airflow pods have enough resources - airflow is witten in Python and can also be resource heavy. It also can be done via monitoring.

Another possible thing to check is product/project DAGs. If they are written incorrectly, it can lead to excessive resource load, see https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html .

Also the issue can be on airflow side especially if it appeared after airflow upgrade, for example: https://github.com/apache/airflow/issues/33688 .

### DBAAS configuration related issues

For DBAAS integration airflow uses [custom secrets backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html#roll-your-own-secrets-backend).

For troubleshooting DBAAS/MAAS integration it is necessary to be familiar with [DBAAS preinstall job script](/docker/create_dbs_dbaas.py) (used to create DB via DBAAS before airflow installation) and with [qubership airflow secrets bakcend DBAAS package](/docker/dbaasintegrationpackage). 

**Note**: It is possible to enable more logging in the DBaaS integration package by setting the `DBAAS_INTEGRATION_LOG_LEVEL` environment variable to `DEBUG`. The `config.logging.logging_level` parameter must also be set to debug in this case.

Before investigating the problem, it is necessary to check if there were any changes done to dbaas integration/script on product side.

Next thing to check is DBAAS(and MAAS if it's used) logs. It is possible to search DBAAS logs by the namespace or service name. PG logs are also worth checking.

If the issue is only related to airflow connections(not main PG/airflow connections), it is also necessary to completely validate `config.secrets.backend_kwargs` - if it is specified correctly and if it is correctly passed to installer.

Also, if the problem only happens in preinstall job and it is not possible to read it's log because it is getting deleted too fast, it is possible to overwrite `.Values.customPreinstallJob.command` and `.Values.customPreinstallJob.args` installation parameters to make the job sleep for 10 minutes. After that it should be possible to access job pod terminal and run dbaas integration script from terminal - with this approach it is possible to see the logs.

If you are seeing SSL errors of different types, it is necessary to check TLS configuration: that certificates are correctly mounted into the pods, that paths to certificates are correctly specified and the services that airflow uses have correct certificates. It is also possible to disable certificate validations to check if the problem is really with certificate.


### IDP Configuration related issues

By default, for IDP(Keycloak) integration airflow uses [webserver_config_keycloak.py](/chart/helm/airflow/qs_files/webserver_config_keycloak.py). In installation parameters it is specified like the following:

```yaml
webserver:
...
  webserverConfig: |-
    {{ .Files.Get "qs_files/webserver_config_keycloak.py" }}
...
```

Before investigating the problem it is necessary to verify that in installation parameters it is specified connection. It is also necessary to check if there are any changes made on uder side to `qs_files/webserver_config_keycloak.py`.

Next step is to validate IDP keycloak configuration: that client is correct, that client redirect uri parameter is correct, that there are users in keycloak and that these users in keycloak have correct roles. It is also necessary to check airflow environment variables and that they have correct values.

It is also necessary to try logging in to airflow and check URLs in your browser and if they correspond to idp integration configuration.

### Logging Config Issues

Custom logging config class supported by platform is located at [qs_platform_logging_config.py](/chart/helm/airflow/qs_files/qs_platform_logging_config.py). It is based on community logging config from https://github.com/apache/airflow/blob/main/airflow/config_templates/airflow_local_settings.py . Note, that handlers, specified in `airflow.task` logger are also used to fetch logs in airflow UI (see `class` of the secified handler).
Custom logging config class and related third-party classes usually are used to investigate/troubleshoot issues related to incorrect log format, incorrect number of logs, incorrect logging level.

When troubleshooting task logs in airflow webUI with celery executor, it is necessary to remember that without using S3 storage, logs in UI should be available only when workers are deployed as statefulset with persistence storage. If the workers are deployed differently, absence of airflow logs in UI is expected behavior.

### GitSync container issues

**TBD**

## Evergreen strategy

1) https://github.com/Netcracker/qubership-airflow/blob/main/docker/Dockerfile#L1 .
2) Update airflow version in https://github.com/Netcracker/qubership-airflow/blob/main/docker/requirements.txt (should be the same as in the image) . Check if other lines can be removed
3) Update [qs_platform_logging_config.py](/chart/helm/airflow/qs_files/qs_platform_logging_config.py) based on community logging config from https://github.com/apache/airflow/blob/main/airflow/config_templates/airflow_local_settings.py .
4) Check that airflow platform image and airflow test DAG image can still be built without issues.
5) Update airflow helm chart based on https://github.com/apache/airflow/tree/main/chart while keeping qubership changes.
6) Update the chart-version in the pipeline .github/workflows/helm-charts-release.yaml when the chart is upgraded.
7) Update versions of statsd-exporter and git-sync in the chart\helm\airflow\release-images.yaml when the statsd-exporter and git-sync are upgraded.
8) Check that airflow can still be deployed.
9) Check that DBAAS/IDP integrations still work.
10) Check that test DAGs in test DAG image still work.
11) Update gitSync image based on community recommended image (`images.gitSync.tag` parameter in community helm chart).
12) Check that gitSync still works
13) Update community rclone image to the [latest released](https://github.com/rclone/rclone/releases): https://github.com/Netcracker/qubership-airflow/blob/main/rclone-image/Dockerfile .
14) Check that rclone still works
15) Update status provisioner image to the [latest released](https://github.com/Netcracker/qubership-deployment-status-provisioner/releases).
16) Update [integration-tests base image](https://github.com/Netcracker/qubership-airflow/blob/main/integration-tests/tests/docker/Dockerfile) based on the [latest released](https://github.com/Netcracker/qubership-docker-integration-tests/releases).
17) Update sitemanager base linux image to the latest released https://github.com/Netcracker/qubership-airflow/blob/main/site-manager/docker/Dockerfile#L1 and https://github.com/Netcracker/qubership-airflow/blob/main/site-manager/docker/Dockerfile#L23 .
18) Update go version in https://github.com/Netcracker/qubership-airflow/blob/main/site-manager/go.mod#L3 . It should be the same as in dockerfile.
19) Update [disaster recovery daemon version](https://github.com/Netcracker/qubership-airflow/blob/main/site-manager/go.mod#L8) to the [latest released](https://github.com/Netcracker/qubership-disaster-recovery-daemon/releases)
20) If there are specific Golang library vulnerabilities, address them in https://github.com/Netcracker/qubership-airflow/blob/main/site-manager/go.mod .
21) Let GoLand/PyCharm update go.mod/go.sum files of status provisioner based on your previous changes
22) Check that sitemanager still can be built and works
23) Check remaining vulnerabilities and address them.
24) Update the documentation with the new version (places for replacing can be found by the old airflow version).









