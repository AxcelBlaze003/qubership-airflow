import boto3


def add_lifecycle_policy():
    # specify connection parameters
    s3_resource = boto3.resource(
        "s3",
        endpoint_url="http://minio.s3.qubership.cloud:80",
        aws_access_key_id="minioaccesskey",
        aws_secret_access_key="miniosecret",
    )
    airflow_logs_bucket = None
    for bucket in s3_resource.buckets.all():
        # specify bucket name
        if bucket.name == "airflow_logs_bucket":
            airflow_logs_bucket = bucket

    lifecycle_configuration = {
        "Rules": [
            {
                "Expiration": {
                    # specify expiration time
                    "Days": 1,
                },
                "Filter": {
                    # specify log folder name
                    "Prefix": "fenrirk8s/",
                },
                "ID": "TestOnly",
                "Status": "Enabled",
            },
        ],
    }
    airflow_logs_bucket.LifecycleConfiguration().put(
        LifecycleConfiguration=lifecycle_configuration
    )


if __name__ == "__main__":
    add_lifecycle_policy()
