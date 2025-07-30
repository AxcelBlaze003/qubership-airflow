FROM python:3.12-slim AS builder

WORKDIR /app

COPY docker-transfer/sync_util.py /app/sync_util.py
COPY chart /app/chart
COPY docs /app/docs
COPY tests_dags_image/dags/dr_sync_folderv2.py /app/dr_sync_folderv2.py
COPY tests_dags_image/dags /app/dags

RUN pip install --no-cache-dir ruamel.yaml && \
    python sync_util.py images chart/helm/airflow/release-images.yaml chart/helm/airflow/values.yaml && \
    python sync_util.py images chart/helm/airflow/release-images.yaml chart/helm/airflow/charts/airflow-site-manager/values.yaml && \
    python sync_util.py images chart/helm/airflow/release-images.yaml chart/helm/airflow/charts/integrationTests/values.yaml && \
    python sync_util.py chart-version chart/helm/airflow/charts/airflow-site-manager/Chart.yaml chart/helm/airflow/charts/airflow-site-manager/values.yaml && \
    python sync_util.py chart-version chart/helm/airflow/charts/integrationTests/Chart.yaml chart/helm/airflow/charts/integrationTests/values.yaml

FROM scratch

# Transfer
COPY --from=builder /app/chart /chart
COPY --from=builder /app/docs /docs
COPY --from=builder /app/dr_sync_folderv2.py /dr_sync_folderv2.py
COPY --from=builder /app/dags /dags