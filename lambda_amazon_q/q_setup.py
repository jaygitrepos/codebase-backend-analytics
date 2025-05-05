import boto3
import os
import json
import logging
import cfnresponse
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Creates an Amazon Q dataset connected to the Athena table
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Get environment variables
    database_name = os.environ.get('DATABASE_NAME')
    table_name = os.environ.get('TABLE_NAME')
    aws_account_id = os.environ.get('AWS_ACCOUNT_ID')
    region = os.environ.get('REGION')
    
    try:
        # Initialize Amazon Q client
        q_client = boto3.client('qbusiness')
        
        # Define Amazon Q dataset name
        dataset_name = f"{database_name}-{table_name}"
        dataset_display_name = "Events Data Analysis"
        
        # Check if dataset already exists
        try:
            # List existing datasets to check if our dataset exists
            response = q_client.list_datasets(
                maxResults=100  # Adjust as needed
            )
            
            datasets = response.get('datasets', [])
            existing_dataset_id = None
            
            for dataset in datasets:
                if dataset.get('name') == dataset_name:
                    existing_dataset_id = dataset.get('id')
                    logger.info(f"Dataset {dataset_name} already exists with ID: {existing_dataset_id}")
                    break
                
            if existing_dataset_id:
                # Update existing dataset
                update_response = q_client.update_dataset(
                    applicationId="default",  # Replace with your Q application ID if different
                    datasetId=existing_dataset_id,
                    displayName=dataset_display_name,
                    description="Iceberg table dataset for analytics with Amazon Q",
                    roleArn=f"arn:aws:iam::{aws_account_id}:role/service-role/AmazonQServiceRole-default"  # Replace with appropriate role
                )
                logger.info(f"Updated dataset: {update_response}")
                dataset_id = existing_dataset_id
            else:
                # Create new dataset for Athena
                create_response = q_client.create_dataset(
                    applicationId="default",  # Replace with your Q application ID if different
                    name=dataset_name,
                    displayName=dataset_display_name,
                    description="Iceberg table dataset for analytics with Amazon Q",
                    roleArn=f"arn:aws:iam::{aws_account_id}:role/service-role/AmazonQServiceRole-default",  # Replace with appropriate role
                    dataSourceConfiguration={
                        'athenaConfiguration': {
                            'databaseName': database_name,
                            'tableName': table_name,
                            'workgroupName': 'primary'  # Adjust based on your Athena setup
                        }
                    }
                )
                logger.info(f"Created dataset: {create_response}")
                dataset_id = create_response.get('datasetId')
            
            # Now let's verify if we need to create/update an Amazon Q application to use this dataset
            try:
                # Check if there's an existing application or create a new one if necessary
                app_response = q_client.list_applications(
                    maxResults=10
                )
                
                applications = app_response.get('applications', [])
                app_id = None
                
                if applications:
                    # Use the first available application
                    app_id = applications[0].get('id')
                    logger.info(f"Using existing Amazon Q application: {app_id}")
                    
                    # Update the application to include our dataset
                    app_update = q_client.update_application(
                        applicationId=app_id,
                        dataSourceIds=[dataset_id]  # Include our dataset
                    )
                    logger.info(f"Updated application with our dataset: {app_update}")
                else:
                    # No applications found, which is unusual since we should have a default one
                    # We could create one, but that's typically done through the console
                    logger.warning("No Amazon Q applications found. Please create one through the console.")
                
                # Return success response with dataset details
                response_data = {
                    'Message': 'Amazon Q dataset setup completed',
                    'DatasetId': dataset_id,
                    'DatasetName': dataset_name,
                    'ApplicationId': app_id if app_id else 'Not found'
                }
                return cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
                
            except ClientError as app_error:
                logger.error(f"Error setting up Amazon Q application: {app_error}")
                return cfnresponse.send(event, context, cfnresponse.FAILED, {
                    'Error': f"Dataset created but application setup failed: {str(app_error)}"
                })
            
        except ClientError as e:
            logger.error(f"Error listing or creating datasets: {e}")
            return cfnresponse.send(event, context, cfnresponse.FAILED, {
                'Error': str(e)
            })
            
    except Exception as e:
        logger.error(f"Error setting up Amazon Q dataset: {str(e)}")
        return cfnresponse.send(event, context, cfnresponse.FAILED, {
            'Error': str(e)
        })