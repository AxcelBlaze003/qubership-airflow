import os
import time

from PlatformLibrary import PlatformLibrary

environ = os.environ
managed_by_operator = "true"
namespace = environ.get("AIRFLOW_NAMESPACE")
api_server_component = "api-server"
dagprocessor_component = "dag-processor"
worker_component = "worker"
scheduler_component = "scheduler"
worker_service = environ.get("WORKER_SERVICE_NAME")
timeout = 300

if __name__ == "__main__":
    time.sleep(10)
    print(
        "Checking airflow-api-server deployment and worker deployment/stateful set are ready"
    )
    try:
        k8s_lib = PlatformLibrary(managed_by_operator)
    except Exception as e:
        print(e)
        exit(1)
    timeout_start = time.time()
    # Check type of worker
    worker_deployments, worker_stateful = None, None
    try:
        worker_deployments = k8s_lib.get_deployment_entities_count_for_service(
            namespace, worker_component, label="component"
        )
        if not worker_deployments:
            worker_stateful = k8s_lib.get_stateful_set_replicas_count(
                worker_service, namespace
            )
    except:  # noqa E722
        print("Deployment or stateful set for worker is not found")
    while time.time() < timeout_start + timeout:
        try:
            deployments = k8s_lib.get_deployment_entities_count_for_service(
                namespace, api_server_component, label="component"
            )
            ready_deployments = (
                k8s_lib.get_active_deployment_entities_count_for_service(
                    namespace, api_server_component, label="component"
                )
            )
            print(
                f"[Check status] Api server deployments: {deployments}, ready deployments: {ready_deployments}"
            )
            scheduler_deployments = k8s_lib.get_deployment_entities_count_for_service(
                namespace, scheduler_component, label="component"
            )
            scheduler_ready_deployments = (
                k8s_lib.get_active_deployment_entities_count_for_service(
                    namespace, scheduler_component, label="component"
                )
            )
            print(
                f"[Check status] Scheduler deployments: {scheduler_deployments},"
                f" ready deployments: {scheduler_ready_deployments}"
            )
            dag_processor_deployments = (
                k8s_lib.get_deployment_entities_count_for_service(
                    namespace, dagprocessor_component, label="component"
                )
            )
            dag_processor_ready_deployments = (
                k8s_lib.get_active_deployment_entities_count_for_service(
                    namespace, dagprocessor_component, label="component"
                )
            )
            print(
                f"[Check status] DAG processor deployments: {dag_processor_deployments},"
                f" ready deployments: {dag_processor_ready_deployments}"
            )
            if worker_deployments:
                worker_count = k8s_lib.get_deployment_entities_count_for_service(
                    namespace, worker_component, label="component"
                )
                worker_ready = k8s_lib.get_active_deployment_entities_count_for_service(
                    namespace, worker_component, label="component"
                )
                print(
                    f"[Check status] Worker deployments: {worker_count}, ready deployments: {worker_ready}"
                )
            elif worker_stateful:
                worker_count = k8s_lib.get_stateful_set_replicas_count(
                    worker_service, namespace
                )
                worker_ready = k8s_lib.get_stateful_set_ready_replicas_count(
                    worker_service, namespace
                )
                print(
                    f"[Check status] Worker stateful set: {worker_count}, ready replicas: {worker_ready}"
                )
        except Exception as e:
            print(e)
            continue
        if worker_deployments or worker_stateful:
            if (
                deployments == ready_deployments
                and deployments != 0
                and worker_count == worker_ready
                and scheduler_deployments == scheduler_ready_deployments
                and dag_processor_deployments == dag_processor_ready_deployments
            ):
                print(
                    "Airflow-api-server deployment, scheduler deployment and worker are ready"
                )
                time.sleep(20)
                exit(0)
        else:
            if (
                deployments == ready_deployments
                and deployments != 0
                and scheduler_deployments == scheduler_ready_deployments
                and dag_processor_deployments == dag_processor_ready_deployments
            ):
                print(
                    "Airflow-api-server deployment and scheduler deployment are ready, worker doesn't present"
                )
                time.sleep(20)
                exit(0)
        time.sleep(10)
    print(
        f"Airflow-api-server deployment, scheduler deployment or worker is not ready at least {timeout} seconds"
    )
    exit(1)
