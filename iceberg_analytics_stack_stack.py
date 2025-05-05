from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_quicksight as quicksight,
    aws_kinesis as kinesis,
    aws_kinesisfirehose as firehose,
    CfnOutput,
    RemovalPolicy,
    CustomResource,
    custom_resources,
    Duration,
    aws_amplify as amplify,
    aws_secretsmanager as secretsmanager,
    Fn,
    aws_s3_deployment as s3deploy
)
from constructs import Construct
from datetime import datetime

class IcebergAnalyticsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, quicksight_identity_region=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Set QuickSight identity region (defaults to stack region if not provided)
        self.quicksight_identity_region = quicksight_identity_region or self.region
        
        # 1. Create S3 bucket for Iceberg data
        iceberg_bucket = s3.Bucket(
            self, "iceberg-user-analytics-data",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # 2. Create Glue Database (Iceberg catalog)
        iceberg_database = glue.CfnDatabase(
            self, "IcebergDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="iceberg_analytics_db",
                description="Iceberg database for analytics data"
            )
        )

        # 3. Create Glue Table with Iceberg format
        iceberg_table = glue.CfnTable(
            self, "IcebergTable",
            catalog_id=self.account,
            database_name=iceberg_database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="events_table",
                description="Events data in Iceberg format",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "table_type": "ICEBERG",
                    "format": "iceberg"
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{iceberg_bucket.bucket_name}/events/",
                    input_format="org.apache.hadoop.mapred.InputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="userid", type="string"),
                        glue.CfnTable.ColumnProperty(name="eventname", type="string"),
                        glue.CfnTable.ColumnProperty(name="pagename", type="string"),
                        glue.CfnTable.ColumnProperty(name="eventdate", type="date"),
                        glue.CfnTable.ColumnProperty(name="campaigncode", type="string")
                    ]
                )
            )
        )

        # 4. Create Kinesis Data Stream
        data_stream = kinesis.Stream(
            self, "EventsDataStream",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            retention_period=Duration.hours(24)
        )

        # 5. Create role for Firehose with permissions to write to Iceberg
        firehose_role = iam.Role(
            self, "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com")
        )
        
        # Grant Firehose permissions to read from Kinesis
        data_stream.grant_read(firehose_role)
        
        # Grant Firehose permissions to write to S3
        iceberg_bucket.grant_read_write(firehose_role)
        
        # Grant Firehose permissions to use Glue catalog and write to Iceberg table
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetTable",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions",
                    "glue:GetTables",
                    "glue:UpdateTable",
                    "glue:BatchCreatePartition",
                    "glue:CreatePartition",
                    "glue:UpdatePartition"
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{iceberg_database.ref}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{iceberg_database.ref}/events_table"
                ]
            )
        )
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kinesis:DescribeStream",
                    "kinesis:GetShardIterator",
                    "kinesis:GetRecords",
                    "kinesis:ListShards"
                ],
                resources=[
                    f"arn:aws:kinesis:{self.region}:{self.account}:stream/*",                    
                ]
            )
        )
        policy_dependency = firehose_role.node.find_child('DefaultPolicy')
        # 6. Create Firehose delivery stream that writes to Iceberg
        firehose_stream = firehose.CfnDeliveryStream(
            self, "EventsFirehoseStream",
            delivery_stream_name="events-to-iceberg",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=data_stream.stream_arn,
                role_arn=firehose_role.role_arn
            ),
            
            iceberg_destination_configuration=firehose.CfnDeliveryStream.IcebergDestinationConfigurationProperty(
                catalog_configuration=firehose.CfnDeliveryStream.CatalogConfigurationProperty(
                      catalog_arn="arn:aws:glue:"+self.region+":"+self.account+":catalog"
                ),
                role_arn=firehose_role.role_arn,
#                catalog_id=self.account,
#                table_name="events_table",
#                database_name=iceberg_database.ref,
                retry_options=firehose.CfnDeliveryStream.RetryOptionsProperty(
                          duration_in_seconds=123
                ),
                
                s3_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                    bucket_arn=iceberg_bucket.bucket_arn,
                    role_arn=firehose_role.role_arn,
                    prefix="firehose/errors/",
                    error_output_prefix="firehose/errors/",
                    buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                        interval_in_seconds=60,
                        size_in_m_bs=5
                    )
                ),
                destination_table_configuration_list=[firehose.CfnDeliveryStream.DestinationTableConfigurationProperty(
                    destination_database_name=iceberg_database.ref,
                    destination_table_name="events_table",
                    # the properties below are optional
                    s3_error_output_prefix="s3ErrorOutputPrefix",
                    unique_keys=["uniqueKeys"]
                )],
            )
        )
        firehose_stream.node.add_dependency(policy_dependency)
        # 7. Create Lambda function for data processing with permission to write to Kinesis
        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )
        
        # Grant Lambda permissions to write to Kinesis Data Stream
        data_stream.grant_write(lambda_role)
        
        # Other permissions for Lambda
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:ListBucket",
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                resources=[
                    iceberg_bucket.bucket_arn,
                    f"{iceberg_bucket.bucket_arn}/*"
                ]
            )
        )

        lambda_function = lambda_.Function(
            self, "IcebergDataProcessor",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda"),
            handler="iceberg_processor.handler",
            role=lambda_role,
            environment={
                "KINESIS_STREAM_NAME": data_stream.stream_name,
                "ICEBERG_BUCKET": iceberg_bucket.bucket_name,
                "DATABASE_NAME": iceberg_database.ref,
                "TABLE_NAME": "events_table"
            },
            timeout=Duration.minutes(3)
        )

        # 8. QuickSight setup with Amazon Q Pro
        # Create a Lambda function to handle QuickSight user creation and Q Pro subscriptions
        quicksight_setup_role = iam.Role(
            self, "QuickSightSetupRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ]
        )
        
        # Add custom policies for QuickSight user management and Amazon Q
        quicksight_setup_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "quicksight:CreateUser",
                    "quicksight:RegisterUser",
                    "quicksight:CreateAdmin",
                    "quicksight:CreateNamespace",
                    "quicksight:Subscribe",
                    "quicksight:CreateAccountSubscription",
                    "quicksight:UpdateUser",
                    "quicksight:UpdateSubscription",
                    "quicksight:CreateGroup",
                    "quicksight:CreateGroupMembership",
                    "quicksight:DescribeAccountSubscription",
                    "sso:ListInstances",
                    'ds:CreateAlias',
                    'ds:AuthorizeApplication',
                    'ds:UnauthorizeApplication',
                    'ds:CreateIdentityPoolDirectory',
                    'ds:CreateDirectory',
                    'ds:DescribeDirectories',
                    
                ],
                resources=["*"]  # Scope down in production
            )
        )

        # Lambda function to create QuickSight users and subscribe to Amazon Q Pro
        quicksight_setup_lambda = lambda_.Function(
            self, "QuickSightSetupFunction",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda_quicksight"),
            handler="quicksight_setup.handler",
            role=quicksight_setup_role,
            timeout=Duration.minutes(5),
            environment={
                "QUICKSIGHT_IDENTITY_REGION": self.quicksight_identity_region,
                "AWS_ACCOUNT_ID": self.account,
                "USERS": "user1@example.com,user2@example.com,user3@example.com"  # Customize with your users
            }
        )
        
        # Custom resource to trigger QuickSight setup
        quicksight_provider = custom_resources.Provider(
            self, "QuickSightSetupProvider",
            on_event_handler=quicksight_setup_lambda,
        )
        
        quicksight_setup_resource = CustomResource(
            self, "QuickSightSetupResource",
            service_token=quicksight_provider.service_token,
            properties={
                "timestamp": str(datetime.now())  # Force execution on each deployment
            }
        )

        artifacts_bucket = s3.Bucket(
            self, 
            "BuildArtifactsBucket",
            auto_delete_objects=True,  # Optional: Clean up on stack deletion
            removal_policy=RemovalPolicy.DESTROY  # Optional: Delete bucket on stack deletion
        )
        
        # Deploy local build artifacts to S3
        deployment = s3deploy.BucketDeployment(
            self,
            "DeployBuildArtifacts",
            sources=[s3deploy.Source.asset(local_build_path)],
            destination_bucket=artifacts_bucket,
            destination_key_prefix="build"
        )
        
        # Create role for Amplify
        amplify_role = iam.Role(
            self,
            "AmplifyS3AccessRole",
            assumed_by=iam.ServicePrincipal("amplify.amazonaws.com")
        )
        
        # Grant Amplify access to the bucket
        artifacts_bucket.grant_read(amplify_role)
        
        # Create the Amplify app
        amplify_app = amplify.CfnApp(
            self,
            "MyAmplifyApp",
            name="my-automated-local-build-app",
            iam_service_role=amplify_role.role_arn,
            # No build spec needed for manual deployments
        )
        
        # Create a branch for deployment
        main_branch = amplify.CfnBranch(
            self,
            "MainBranch",
            app_id=amplify_app.attr_app_id,
            branch_name="main",
            enable_auto_build=False,  # No auto-build from git
            stage="PRODUCTION"
        )
        main_branch.add_dependency(amplify_app)
        
        # Create a custom resource to start the deployment after everything is set up
        deploy_to_amplify = custom_resources.AwsCustomResource(
            self,
            "DeployToAmplify",
            on_create=custom_resources.AwsSdkCall(
                service="Amplify",
                action="startDeployment",
                parameters={
                    "appId": amplify_app.attr_app_id,
                    "branchName": "main",
                    "sourceUrl": f"s3://{artifacts_bucket.bucket_name}/build"
                },
                physical_resource_id=custom_resources.PhysicalResourceId.of("AmplifyDeployment")
            ),
            policy=custom_resources.AwsCustomResourcePolicy.from_sdk_calls(
                resources=custom_resources.AwsCustomResource,
				policy=custom_resources.AwsCustomResourcePolicy.from_sdk_calls(
                resources=custom_resources.AwsCustomResourcePolicy.ANY_RESOURCE
            )
        )
        
        # Make sure the deployment happens after the branch is created and artifacts are uploaded
        deploy_to_amplify.node.add_dependency(main_branch)
        deploy_to_amplify.node.add_dependency(deployment)
        
        # Outputs for reference
        CfnOutput(
            self,
            "AmplifyAppId",
            value=amplify_app.attr_app_id,
            description="Amplify App ID"
        )