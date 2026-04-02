import json
import urllib3
import os

# Reuse a single HTTP connection pool for efficiency across Lambda invocations.
http = urllib3.PoolManager()

# Slack webhook URL is provided via environment variable.
SLACK_WEBHOOK = os.environ['SLACK_WEBHOOK']


def lambda_handler(event, context):
    """
    AWS Lambda function that forwards SNS alerts to Slack.

    Workflow:
      1. Triggered by SNS event.
      2. Extract message from each SNS record.
      3. Format message for Slack.
      4. Send POST request to Slack webhook.

    This acts as a lightweight bridge between AWS alerting (SNS)
    and Slack notifications.
    """
    for record in event['Records']:
        # SNS delivers messages in a list of records.
        message = record['Sns']['Message']
        
        # Format message for Slack (simple text payload).
        slack_message = {
            "text": f"Alert:\n{message}"
        }

        # Send message to Slack via webhook.
        http.request(
            "POST",
            SLACK_WEBHOOK,
            body=json.dumps(slack_message),
            headers={"Content-Type": "application/json"}
        )

    # Return success response for Lambda execution logs.
    return {"statusCode": 200}