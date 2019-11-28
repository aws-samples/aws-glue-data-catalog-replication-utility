# Simplifying Glue Data Catalog Replication across AWS accounts

This is a source code for Cross-account AWS Glue Data Catalog replication Utility demonstrated in the following diagram. It is based on AWS Glue API / SDK for Java.
![Alt](./src/test/resources/Glue_Synchronization.png)

## Build Instructions

1. The source code has Maven nature, so if you have Maven locally then you can build it using standard Maven commands e.g. ```mvn -X clean install```. or use the options available in your IDE
2. Use the generated Jar file to deploy a Lambda function

## AWS Service Requirements
The following AWS services are required to deploy this replication utility.
### Source Account
 - 3 export Lambda Functions 
 - 1 Lambda Execution IAM Role
 - 3 DynamoDB tables
 - 2 SNS Topics
 - 1 SQS Queue
 - 1 S3 Bucket

### Each Consumption Account
 - 3 import Lambda Functions 
 - 1 Lambda Execution IAM Role
 - 2 DynamoDB tables
 - 2 SQS Queues
 -  Cross-account permissions for import Lambda Function to receive messages from SNS Topic.
 -  Cross-account permissions on S3 Bucket for Large Table Import Lambda's IAM role.

## Lambda Functions Overview
| Class                                                         | Overview 	   |
|-------------------------------------------------------------- | -------------- |
| [GDCReplicationPlanner](./src/main/java/com/amazonaws/gdcreplication/lambda/GDCReplicationPlanner.java) | Lambda Function to export list of databases from Glue Data Catalog in Source Account.|
| [ExportDatabaseWithTables](./src/main/java/com/amazonaws/gdcreplication/lambda/ExportDatabaseWithTables.java) | Lambda Function to export a database and all of its tables from Glue Data Catalog in Source Account.|
| [ExportLargeTable](./src/main/java/com/amazonaws/gdcreplication/lambda/ExportLargeTable.java) | Lambda Function to export a large table (table with more than 10 partitions) from Glue Data Catalog in Source Account.|
| [ImportDatabaseOrTable](./src/main/java/com/amazonaws/gdcreplication/lambda/ImportDatabaseOrTable.java) | Lambda Function to import a database or a table to Glue Data Catalog in Target Account.|
| [ImportLargeTable](./src/main/java/com/amazonaws/gdcreplication/lambda/ImportLargeTable.java) | Lambda Function to import a large table to Glue Data Catalog in Target Account.|
| [DLQImportDatabaseOrTable](./src/main/java/com/amazonaws/gdcreplication/lambda/DLQImportDatabaseOrTable.java) | Dead Letter Queue Processing - Lambda Function to import a database or a table to Glue Data Catalog in Target Account.|

## Lambda Environment Variables Summary
### GDCReplicationPlanner Lambda
| Variable Name                    	| Variable Value          	|
|----------------------------------	|-------------------------	|
| source_glue_catalog_id           	| Source AWS Account Id      |
| ddb_name_gdc_replication_planner 	| Name of the DDB Table      |
| database_prefix_list             	| e.g. raw_data_,processed_data_ The list of database prefixes to be replicated. |
| separator                        	| e.g. ,                       	|
| region                           	| e.g. us-east-1               	|
| sns_topic_arn_gdc_replication_planner |  Name of the SNS Topic   |

### ExportDatabaseWithTables Lambda
| Variable Name                    	| Variable Value          	|
|----------------------------------	|-------------------------	|
| source_glue_catalog_id           	| Source AWS Account Id     	|
| ddb_name_db_export_status 	        | Name of the DDB Table     |
| ddb_name_table_export_status       | Name of the DDB Table     |
| region             	            | e.g. us-east-1  	       |
| sns_topic_arn_export_dbs_tables    | Name of the SNS Topic    |

### ExportLargeTable Lambda
| Variable Name                    	| Variable Value          	|
|----------------------------------	|-------------------------	|
| s3_bucket_name 	                | Name of the S3 Bucket used to save partitions for large Tables |
| ddb_name_table_export_status       | Name of the DDB Table     |
| region             	            | e.g. us-east-1  	       |
| sns_topic_arn_export_dbs_tables    | Name of the SNS Topic    |

### ImportDatabaseOrTable Lambda
| Variable Name                    	| Variable Value          	|
|----------------------------------	|-------------------------	|
| target_glue_catalog_id           	| Target AWS Account Id    	|
| ddb_name_db_import_status 	        | Name of the DDB Table      |
| ddb_name_table_import_status       | Name of the DDB Table  |
| skip_archive             	        | true 	                 |
| dlq_url_sqs                        | Name of the SQS Queue  |
| region             	            | e.g. us-east-1  	     |

### ImportLargeTable Lambda
| Variable Name                    	| Variable Value         |
|----------------------------------	|----------------------	 |
| target_glue_catalog_id           	| Target AWS Account Id  |
| ddb_name_table_import_status      | Name of the DDB Table  |
| skip_archive             	        | true 	                 |
| region             	            | e.g. us-east-1  	     |

### DLQImportDatabaseOrTable Lambda
| Variable Name                    	| Variable Value          |
|----------------------------------	|-------------------------	|
| target_glue_catalog_id             | Target AWS Account Id     |
| ddb_name_db_import_status 	        | Name of the DDB Table     |
| ddb_name_table_import_status       | Name of the DDB Table    |
| skip_archive             	        | true 	                    |
| dlq_url_sqs                        | Name of the SQS Queue   |
| region             	            | e.g. us-east-1  	      |

## DynamoDB Tables
| Table             | Description 	 |  Account   | Schema 	    |  Capacity      | 
|-------------------|----------------|------------|------------ | -------------- |
| glue_database_export_task | audit data for replication planner | source account | Partition key - db_id (String), Sort key - export_run_id (Number) | On-Demand |
| db_status | audit data for databases exported | source account | Partition key - db_id (String), Sort key - export_run_id (Number) | On-Demand |
| table_status | audit data for tables exported | source account | Partition key - table_id (String), Sort key - export_run_id (Number) | On-Demand |
| db_status | audit data for databases imported | target account | Partition key - db_id (String), Sort key - import_run_id (Number) | On-Demand |
| table_status | audit data for tables imported | target account | Partition key - table_id (String), Sort key - import_run_id (Number) | On-Demand |

## Deployment Instructions
The deployment sequence is follows:
1. Create SNS Topics in Source Account
2. Create a S3 Bucket to be used to save partitions for large Tables
3. Create SQS Queue (Large Table SQS Queue) of type Standard in Source Account
4. Create DynamoDB tables in Source Account
5. Create Lambda Execution Role in Source Account to be used by Lambda Functions in Source Account
6. Deploy **GDCReplicationPlanner** Lambda Function in Source Account 
7. Deploy **ExportDatabaseWithTables** Lambda Function in Source Account 
8. Deploy **ExportLargeTable** Lambda Function in Source Account
9. Add **Large Table SQS Queue** as a trigger to **ExportLargeTable** Lambda Function
10. Add SNS Topic as a trigger to **ExportDatabaseWithTables** Lambda function
11. Create CloudWatch Event Rule in Source Account and add Export Lambda function as the target
12. Cross-Account Permissions Step 1 of 2: in Source Account. Grant permissions to account B to subscribe to the second SNS Topic:

	```
	aws sns add-permission --label lambda-access --aws-account-id TargetAccount \
	--topic-arn arn:aws:sns:us-east-1:SourceAccount:GlueExportSNSTopic \
	--action-name Subscribe ListSubscriptionsByTopic Receive
	```
	
13. Cross-Account Permissions Step 2 of 2: From account B, add the Lambda permission to allow invocation from Amazon SNS.
	
	```
	aws lambda add-permission --function-name GlueSynchronizerLambda \
	--source-arn arn:aws:sns:us-east-1:SourceAccount:GlueExportSNSTopic \
	--statement-id sns-x-account --action "lambda:InvokeFunction" \
	--principal sns.amazonaws.com --profile user_sa
	```
	
14. Create a Subscription. From target account, subscribe the Lambda function to the topic. When a message is sent to the lambda-x-account topic in account A, Amazon SNS invokes the SNS-X-Account function in account B.
	
	```
	aws sns subscribe --protocol lambda \
	--topic-arn arn:aws:sns:us-east-1:SourceAccount:GlueExportSNSTopic \
	--notification-endpoint arn:aws:lambda:us-east-1:TargetAccount:function:GlueSynchronizerLambda \
	--profile user_sa
	```
	Additional References:
	 - https://docs.aws.amazon.com/lambda/latest/dg/with-sns-example.html#with-sns-create-x-account-permissions

15. Create DynamoDB tables in Target Account
16. Create SQS queue (Large Table SQS Queue) of type Standard - this is for dead letter queue processing. Default Visibility Timeout = 20 seconds
17. Create SQS (Dead Letter Queue) queue of type Standard - this is to process schema for large tables 
18. Create Lambda Execution Role in Target Account to be used by Lambda Functions in Target Accounts. This needs to have Cross-Account permissions to the S3 bucket created in Step # 2.
19. Deploy **ImportDatabaseOrTable** Lambda Function in Target Account 
20. Deploy **ImportLargeTable** Lambda Function in Target Account 
21. Add **Large Table SQS Queue** as a trigger to **ImportLargeTable** Lambda Function. Batch size =1, Lambda Execution Timeout = 3 minutes, Memory = 192 MB.
22. Deploy **DLQImportDatabaseOrTable** Lambda Function in Target Account
23. Add Dead Letter SQS Queue as a trigger to **DLQImportDatabaseOrTable** Lambda Function. Batch size =1, Lambda Execution Timeout = 15 seconds, Memory = 192 MB.

## Running this solution as a Scheduled Job 
This solution supports the following use cases:

1.	One-time replication of Glue Databases and Tables between one source account to one or more target accounts.
2.	Run this solution as a scheduled Job e.g. once in a day.

Running this solution as a scheduled job means a couple of things 1) replicate new set off Databases and Tables 
2) replicating a table whose definition has changed 3) replicating newly added partitions to a table.

As far as database and tables are concerned, the action taken by Import Lambdas depend on the state of Glue Data Catalog in target account. 
Those actions are summarized in the following table. 

|Input Message Type	| State in Target Glue Data Catalog | Action Taken |
|-------------------|-----------------------------------|------------  |
|Database	| Database exist already					    | Skip the message |
|Database	| Database does not exist 					| Create Database  |
|Table		| Table exist already						| Update Table     | 
|Table		| Table does not exist 						| Create Table     |

As far as partitions are concerned, the process involves the following steps:

|Partitions in Export	| State in Target Glue Data Catalog | Action Taken |
|-----------------------|-----------------------------------|------------  |
|Partitions DO NOT Exist| Target Table has no partitions	| No action to take |
|Partitions DO NOT Exist| Target Table has partitions	    | Delete current partitions |
|Partitions Exist	    | Target Table has no partitions	| Create new partitions |
|Partitions Exist	    | Target Table has partitions		| Delete current partitions, create new partitions |


## License Summary
This sample code is made available under the MIT license. See the LICENSE file.