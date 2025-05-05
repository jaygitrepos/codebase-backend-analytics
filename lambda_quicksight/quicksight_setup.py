import boto3
import os
import json
import logging
import cfnresponse as cfnresponse
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Sets up QuickSight users with Amazon Q Pro subscriptions
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Get environment variables
    identity_region = os.environ.get('QUICKSIGHT_IDENTITY_REGION')
    aws_account_id = os.environ.get('AWS_ACCOUNT_ID')
    users_str = os.environ.get('USERS', '')
    users = [user.strip() for user in users_str.split(',') if user.strip()]
    
    if not users:
        logger.warning("No users specified in environment variables. Skipping QuickSight setup.")
        return cfnresponse.send(event, context, cfnresponse.SUCCESS, {
            'Message': 'No users specified, skipping QuickSight setup'
        })
    
    try:
        # Initialize QuickSight client in the identity region
        quicksight_client = boto3.client('quicksight', region_name=identity_region)
        
        # Check if QuickSight is already subscribed
        try:
            subscription = quicksight_client.describe_account_subscription(
                AwsAccountId=aws_account_id
            )
            logger.info(f"QuickSight subscription exists: {subscription}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Subscribe to QuickSight
                logger.info("QuickSight subscription not found. Creating a new subscription.")
                quicksight_client.create_account_subscription(
                    AwsAccountId=aws_account_id,
                    AccountName=f"QuickSight-{aws_account_id}",
                    Edition='ENTERPRISE',
                    AuthenticationMethod='IAM_AND_QUICKSIGHT',  # or IAM or IAM_AND_QUICKSIGHT
                    NotificationEmail ='notification@example.com',
                    AdminGroup=[
                            'ADMIN',]
                )
                logger.info("Successfully created QuickSight subscription")
            else:
                raise
        
        # Create or update users with Q Pro
        results = []
        for user_email in users:
            username = user_email.split('@')[0]
            
            # Try registering the user first
            try:
                response = quicksight_client.register_user(
                    AwsAccountId=aws_account_id,
                    IdentityType='IAM',
                    Email=user_email,
                    UserRole='ADMIN',  # or AUTHOR, READER based on needs
                    IamArn=f"arn:aws:iam::{aws_account_id}:user/{username}",
                    SessionName=username,
                    QProOptions={
                        'PricingPlan': 'PRO',  # Enable Q Pro subscription
                        'Status': 'ACTIVE'
                    }
                    
                )
                logger.info(f"Registered user {username} with Q Pro: {response}")
                results.append({
                    'username': username,
                    'email': user_email,
                    'status': 'registered_with_q_pro'
                })
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceExistsException':
                    # User exists, update Q Pro subscription
                    try:
                        response = quicksight_client.update_user(
                            AwsAccountId=aws_account_id,
                            Namespace='default',
                            UserName=username,
                            Email=user_email,
                            Role='ADMIN',  # or AUTHOR, READER
                            QProOptions={
                                'PricingPlan': 'PRO',
                                'Status': 'ACTIVE'
                            }
                        )
                        logger.info(f"Updated user {username} with Q Pro: {response}")
                        results.append({
                            'username': username,
                            'email': user_email,
                            'status': 'updated_with_q_pro'
                        })
                    except Exception as update_err:
                        logger.error(f"Failed to update user {username}: {update_err}")
                        results.append({
                            'username': username,
                            'email': user_email,
                            'status': 'update_failed',
                            'error': str(update_err)
                        })
                else:
                    logger.error(f"Failed to register user {username}: {e}")
                    results.append({
                        'username': username,
                        'email': user_email,
                        'status': 'registration_failed',
                        'error': str(e)
                    })
        
        # Set up QuickSight dataset for the Iceberg table
        try:
            # This is a simplified example - in production, you might want to create actual QuickSight datasets
            logger.info("Setting up QuickSight data sources and datasets would go here")
            # Example of how you might create a QuickSight data source for Athena
            # quicksight_client.create_data_source(...)
            # quicksight_client.create_dataset(...)
        except Exception as ds_err:
            logger.warning(f"Failed to set up QuickSight datasets: {ds_err}")
        
        # Return success response
        response_data = {
            'Message': 'QuickSight users setup completed',
            'Results': results
        }
        return cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
        
    except Exception as e:
        logger.error(f"Error setting up QuickSight: {str(e)}")
        return cfnresponse.send(event, context, cfnresponse.FAILED, {
            'Error': str(e)
        })