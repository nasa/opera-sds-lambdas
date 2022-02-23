import re
import boto3
import botocore
import backoff
import traceback


# regexes
NO_MANAGED_FOUND_RE = re.compile(r"No managed instance found")


def is_not_managed_instance(e):
    """Return True if it instance ID is not managed by an ASG,
       i.e. already terminated, detached or never an ASG instance."""

    return True if NO_MANAGED_FOUND_RE.search(str(e)) else False


@backoff.on_exception(
    backoff.expo,
    botocore.exceptions.ClientError,
    max_tries=8,
    max_value=64,
    giveup=is_not_managed_instance,
)
def terminate_instance(client, instance_id):
    client.terminate_instance_in_auto_scaling_group(
        InstanceId=instance_id, ShouldDecrementDesiredCapacity=True
    )


def lambda_handler(event, context):
    print("in lambda_handler")
    print("SQS payload = " + str(event["Records"]))
    c = boto3.client("autoscaling")
    terminated_ids = []
    for record in event["Records"]:
        instance_id = record["body"]
        print("Instance id from SQS is " + instance_id)
        try:
            terminate_instance(c, instance_id)
            terminated_ids.append(instance_id)
        except Exception as e:
            print(f"Exception in calling terminate_instance on {instance_id}: {str(e)}")
            print(traceback.format_exc())
            raise

    return {"statusCode": 200, "body": f"Terminated instances: {terminated_ids}"}
