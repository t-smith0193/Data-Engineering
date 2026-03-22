import json
import urllib3
import os

http = urllib3.PoolManager()

SLACK_WEBHOOK = os.environ['SLACK_WEBHOOK']

def lambda_handler(event, context):
    for record in event['Records']:
        message = record['Sns']['Message']
        
        slack_message = {
            "text": f"Alert:\n{message}"
        }

        http.request(
            "POST",
            SLACK_WEBHOOK,
            body=json.dumps(slack_message),
            headers={"Content-Type": "application/json"}
        )

    return {"statusCode": 200}