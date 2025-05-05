# Iceberg Analytics Stack with Kinesis, QuickSight & Amazon Q

This project contains an AWS CDK stack that creates an end-to-end analytics solution using Apache Iceberg tables with Kinesis Data Streams, Firehose, QuickSight, and Amazon Q integration.

## Architecture

![Architecture Diagram](architecture_diagram.png)

The solution includes the following components:

- **AWS Glue**: Iceberg database and table schema
- **Amazon S3**: Storage for Iceberg data
- **Amazon Kinesis Data Stream**: Real-time data ingestion pipeline
- **Amazon Kinesis Data Firehose**: Stream processor writing to Iceberg tables
- **AWS Lambda**: Data processing and custom resource for setup
- **Amazon QuickSight**: Business Intelligence with Amazon Q Pro integration
- **Amazon Q**: AI-powered analytics assistant for natural language queries

## Prerequisites

1. AWS account with administrative access
2. AWS CDK installed and configured
3. Python 3.9 or higher
4. Node.js 14.x or higher

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd iceberg-analytics-stack
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows, use '.venv\Scripts\activate'
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure the users who should have access to QuickSight and Amazon Q Pro by editing the `USERS` environment variable in `iceberg_analytics_stack_stack.py`.

5. Deploy the stack:
```bash
cdk deploy
```

## Usage

### Sending data to the Kinesis Stream

Data can be sent to the Kinesis stream with the following format:

```json
{
  "eventname": "page_view",
  "pagename": "homepage",
  "eventdate": "2023-06-01",
  "campaigncode": "summer_2023"
}
```

You can use the AWS SDK, CLI, or the Lambda function provided.

### Invoking the Lambda function

You can invoke the Lambda function to process data and send it to the Kinesis stream:

```bash
aws lambda invoke --function-name <function-name> --payload '{"records":[{"eventname":"click","pagename":"product_page","eventdate":"2023-06-01","campaigncode":"summer_2023"}]}' response.json
```

### Accessing QuickSight

The users specified in the stack will automatically be provisioned with QuickSight Enterprise accounts and Amazon Q Pro subscriptions. They can access QuickSight at:

```
https://<region>.quicksight.aws.amazon.com
```

## Directory Structure

```
iceberg-analytics-stack/
│
├── README.md
├── app.py
├── requirements.txt
├── iceberg_analytics_stack/
│   ├── __init__.py
│   └── iceberg_analytics_stack_stack.py
│
├── lambda/
│   └── iceberg_processor.py
│
├── lambda_quicksight/
│   ├── quicksight_setup.py
│   └── cfnresponse.py
│
└── lambda_amazon_q/
    ├── amazon_q_setup.py
    └── cfnresponse.py
```

## Configuration Options

You can modify the following aspects of the stack:

- **QuickSight Identity Region**: If your QuickSight users are provisioned in a different region, specify it in `app.py`.
- **User List**: Update the comma-separated list of email addresses in the `USERS` environment variable.
- **Retention Period**: Modify the Kinesis Data Stream retention period as needed.
- **Table Schema**: Update the Iceberg table schema in `iceberg_analytics_stack_stack.py` to match your data requirements.

## IAM Permissions

The stack creates several IAM roles with specific permissions:
- Lambda execution role for data processing
- Firehose delivery role with permissions to write to Iceberg tables
- QuickSight setup role for user provisioning
- Amazon Q dataset role for connecting to Athena

## Clean Up

To remove all resources created by the stack:

```bash
cdk destroy
```

Note: This will delete all data stored in the S3 buckets created by this stack.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.