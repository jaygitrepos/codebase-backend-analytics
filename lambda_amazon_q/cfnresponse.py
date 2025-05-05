import json
import logging
import urllib.request

SUCCESS = "SUCCESS"
FAILED = "FAILED"

def send(event, context, responseStatus, responseData, physicalResourceId=None, noEcho=False, reason=None):
    responseUrl = event['ResponseURL']

    logging.info(responseUrl)

    responseBody = {
        'Status': responseStatus,
        'Reason': reason or 'See the details in CloudWatch Log Stream: ' + context.log_stream_name,
        'PhysicalResourceId': physicalResourceId or context.log_stream_name,
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'NoEcho': noEcho,
        'Data': responseData
    }

    json_responseBody = json.dumps(responseBody)

    logging.info("Response body:")
    logging.info(json_responseBody)

    headers = {
        'content-type': '',
        'content-length': str(len(json_responseBody))
    }

    try:
        req = urllib.request.Request(responseUrl, json_responseBody.encode('utf-8'), headers)
        with urllib.request.urlopen(req) as response:
            logging.info(f"Status code: {response.getcode()}")
            logging.info(f"Status message: {response.msg}")
        return True
    except Exception as e:
        logging.error(f"send(..) failed executing request.urlopen(..): {str(e)}")
        return False