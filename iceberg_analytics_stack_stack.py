from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_kinesis as kinesis,
    aws_kinesisfirehose as firehose,
    RemovalPolicy,
    Duration,
    aws_logs  as logs
)
from constructs import Construct
from datetime import datetime
import aws_cdk as cdk
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
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{iceberg_bucket.bucket_name}/events/",
                    columns=[
                        glue.CfnTable.ColumnProperty(name="userid", type="string"),
                        glue.CfnTable.ColumnProperty(name="eventname", type="string"),
                        glue.CfnTable.ColumnProperty(name="eventdate", type="timestamp"),
                        glue.CfnTable.ColumnProperty(name="pagename", type="string"),
                        glue.CfnTable.ColumnProperty(name="journeycode", type="string"),
                        glue.CfnTable.ColumnProperty(name="campaigncode", type="string")
                    ]
                )
            ),
            open_table_format_input=glue.CfnTable.OpenTableFormatInputProperty(
                iceberg_input=glue.CfnTable.IcebergInputProperty(
                    metadata_operation="CREATE",
                    version="2"
                )
            )
        )

        

        # 4. Create Kinesis Data Stream
        # Code moved to front end app
        #data_stream = kinesis.Stream(
        #    self, "EventsDataStream",
        #    stream_mode=kinesis.StreamMode.ON_DEMAND,
        #    retention_period=Duration.hours(24)
        #)

        # 5. Create role for Firehose with permissions to write to Iceberg
        firehose_role = iam.Role(
            self, "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com")
        )
        
        # Grant Firehose permissions to read from Kinesis
        #data_stream.grant_read(firehose_role)
        
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
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:PutLogEvents",
                    
                ],
                resources=[
                    "*",                    
                ]
            )
        )
        # Create CloudWatch Log Group and Stream for Firehose errors
        firehose_log_group = logs.LogGroup(
            self, "FirehoseLogGroup",
            log_group_name="/aws/kinesisfirehose/events-to-iceberg",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.ONE_WEEK
        )

        firehose_log_stream = logs.LogStream(
            self, "FirehoseLogStream",
            log_group=firehose_log_group,
            log_stream_name="FirehoseErrors"
        )
        # Add additional permissions for CloudWatch Logs
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:PutLogEvents",
                    "logs:CreateLogStream",
                    "logs:CreateLogGroup",
                    "logs:DescribeLogStreams",
                    "logs:DescribeLogGroups"
                ],
                resources=[
                    firehose_log_group.log_group_arn,
                    f"{firehose_log_group.log_group_arn}:log-stream:*"
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
                kinesis_stream_arn="arn:aws:kinesis:"+self.region+":"+self.account+":stream/webdevcon2025",
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
                          duration_in_seconds=10
                ),
                # Add CloudWatch logging configuration
                cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                    enabled=True,
                    log_group_name=firehose_log_group.log_group_name,
                    log_stream_name=firehose_log_stream.log_stream_name
                ),
                s3_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                    bucket_arn=iceberg_bucket.bucket_arn,
                    role_arn=firehose_role.role_arn,
                    prefix="firehose/errors/",
                    error_output_prefix="firehose/errors/",
                    buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                        interval_in_seconds=60,
                        size_in_m_bs=1
                    )
                ),
                destination_table_configuration_list=[firehose.CfnDeliveryStream.DestinationTableConfigurationProperty(
                    destination_database_name=iceberg_database.ref,
                    destination_table_name="events_table",
                    # the properties below are optional
                    s3_error_output_prefix="s3ErrorOutputPrefix",
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
        #data_stream.grant_write(lambda_role)
        
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

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kinesis:PutRecord",
                ],
                resources=[
                    "arn:aws:kinesis:"+self.region+":"+self.account+":stream/webdevcon2025"
                ]
            )
        )

        lambda_function = lambda_.Function(
            self, "IcebergDataProcessor",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda"),
            handler="iceberg_processor.lambda_handler",
            role=lambda_role,
            environment={
                "KINESIS_STREAM_NAME": "webdevcon2025",
                "ICEBERG_BUCKET": iceberg_bucket.bucket_name,
                "DATABASE_NAME": iceberg_database.ref,
                "TABLE_NAME": "events_table"
            },
            timeout=Duration.minutes(6)
        )

        # 8. Athena results bucket

        athena_results_bucket = s3.Bucket(
            self, 
            "AthenaResultsBucket",
            auto_delete_objects=True,  # Optional: Clean up on stack deletion
            removal_policy=RemovalPolicy.DESTROY  # Optional: Delete bucket on stack deletion
        )

                
        

        