// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.lambda;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.gdcreplication.util.LargeTable;
import com.amazonaws.gdcreplication.util.DDBUtil;
import com.amazonaws.gdcreplication.util.GlueUtil;
import com.amazonaws.gdcreplication.util.SNSUtil;
import com.amazonaws.gdcreplication.util.SQSUtil;
import com.amazonaws.gdcreplication.util.TableWithPartitions;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNSRecord;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * This class has AWS Lambda Handler method. Upon invocation, it gets an SNS Event from source SNS
 * Topic, gets the message(s) from the event. 
 * 
 * For each message, it takes the following actions:
 * 1. Parse the message to database
 * 2. Check if a database exist in Glue
 * 3. If exist, fetches all tables for the database
 * 
 * For each table, it takes the following actions:
 * 1. Convert Glue Table object to JSON String (This is a Table DDL) 
 * 2. Publish the Table DDL to an SNS Topic 
 * 3. Insert a record to a DynamoDB table for status tracking
 * 
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class ExportDatabaseWithTables implements RequestHandler<SNSEvent, Object> {

	@Override
	public String handleRequest(SNSEvent request, Context context) {

		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String sourceGlueCatalogId = Optional.ofNullable(System.getenv("source_glue_catalog_id")).orElse("1234567890");
		String topicArn = Optional.ofNullable(System.getenv("sns_topic_arn_export_dbs_tables"))
				.orElse("arn:aws:sns:us-east-1:1234567890:GlueExportSNSTopic");
		String ddbTblNameForDBStatusTracking = Optional.ofNullable(System.getenv("ddb_name_db_export_status"))
				.orElse("ddb_name_db_export_status");
		String ddbTblNameForTableStatusTracking = Optional.ofNullable(System.getenv("ddb_name_table_export_status"))
				.orElse("ddb_name_table_export_status");
		String sqsQueue4LargeTables = Optional.ofNullable(System.getenv("sqs_queue_url_large_tables")).orElse("");
		int partitionThreshold = 10;
		
		// Client configuration
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);
				
		List<SNSRecord> snsRecods = request.getRecords();
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).build();
		AmazonSNS sns = AmazonSNSClientBuilder.standard().withRegion(region).build();
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();
		
		printEnvVariables(sourceGlueCatalogId, topicArn, ddbTblNameForDBStatusTracking,
				ddbTblNameForTableStatusTracking, sqsQueue4LargeTables);
		System.out.printf("Number of messages in SNS Event: \n" + snsRecods.size());
		processSNSEvent(snsRecods, context, glue, sns, sqs, sourceGlueCatalogId, ddbTblNameForDBStatusTracking,
				ddbTblNameForTableStatusTracking, topicArn, sqsQueue4LargeTables, partitionThreshold);

		return "Message from SNS Topic was processed successfully!";
	}

	/**
	 * This method prints environment variables
	 * @param sourceGlueCatalogId
	 * @param topicArn
	 * @param ddbTblNameForDBStatusTracking
	 * @param ddbTblNameForTableStatusTracking
	 */
	public static void printEnvVariables(String sourceGlueCatalogId, String topicArn,
			String ddbTblNameForDBStatusTracking, String ddbTblNameForTableStatusTracking, String sqsQueue4LargeTables) {
		System.out.println("SNS Topic Arn: " + topicArn);
		System.out.println("Source Catalog Id: " + sourceGlueCatalogId);
		System.out.println("DynamoDB Table for DB Export Auditing: " + ddbTblNameForDBStatusTracking);
		System.out.println("DynamoDB Table for Table Export Auditing: " + ddbTblNameForTableStatusTracking);
		System.out.println("SQS queue for large tables: " + sqsQueue4LargeTables);
	}

	/**
	 * This method processes SNSEvent
	 * @param snsRecods
	 * @param context
	 * @param glue
	 * @param sns
	 * @param sourceGlueCatalogId
	 * @param ddbTblNameForDBStatusTracking
	 * @param ddbTblNameForTableStatusTracking
	 * @param topicArn
	 */
	public static void processSNSEvent(List<SNSRecord> snsRecods, Context context, AWSGlue glue, AmazonSNS sns,
			AmazonSQS sqs, String sourceGlueCatalogId, String ddbTblNameForDBStatusTracking,
			String ddbTblNameForTableStatusTracking, String topicArn, String sqsQueue4LargePartTables,
			int partitionThreshold) {
		Database db = null;
		Gson gson = new Gson();
		DDBUtil ddbUtil = new DDBUtil();
		SNSUtil snsUtil = new SNSUtil();
		GlueUtil glueUtil = new GlueUtil();
		SQSUtil sqsUtil = new SQSUtil();
		long exportRunId = System.currentTimeMillis();
		
		for (SNSRecord snsRecod : snsRecods) {
			
			List<WriteRequest> itemList = new ArrayList<WriteRequest>();
			
			boolean isDatabaseType = false;
			AtomicInteger numberOfTablesExported = new AtomicInteger();
			String databaseDDL = snsRecod.getSNS().getMessage();
			context.getLogger().log("SNS Message Payload: " + databaseDDL);
			Map<String, MessageAttribute> msgAttributeMap = snsRecod.getSNS().getMessageAttributes();
			MessageAttribute msgAttrMessageType = msgAttributeMap.get("message_type");
			MessageAttribute msgAttrExportBatchId = msgAttributeMap.get("export_batch_id");
			
			context.getLogger().log("Message Attribute value: " + msgAttrMessageType.getValue());
			// Convert Message to Glue Database Type
			try {
				if (msgAttrMessageType.getValue().equalsIgnoreCase("database")) {
					db = gson.fromJson(databaseDDL, Database.class);
					isDatabaseType = true;
				}
			} catch (JsonSyntaxException e) {
				System.out.println("Cannot parse SNS message to Glue Database Type.");
				e.printStackTrace();
			}
			if (isDatabaseType) {
				// Check if a database exist in Glue
				Database database = glueUtil.getDatabaseIfExist(glue, sourceGlueCatalogId, db);
				if (Optional.ofNullable(database).isPresent()) {
					PublishResult publishDBResponse = snsUtil.publishDatabaseSchemaToSNS(sns, topicArn, databaseDDL,
							sourceGlueCatalogId, msgAttrExportBatchId.getValue());
					if (Optional.ofNullable(publishDBResponse.getMessageId()).isPresent()) {
						System.out.println("Database schema published to SNS Topic. Message_Id: "
								+ publishDBResponse.getMessageId());
						ddbUtil.trackDatabaseExportStatus(ddbTblNameForDBStatusTracking, db.getName(), databaseDDL,
								publishDBResponse.getMessageId(), sourceGlueCatalogId, exportRunId, msgAttrExportBatchId.getValue(), true);
					} else {
						ddbUtil.trackDatabaseExportStatus(ddbTblNameForDBStatusTracking, db.getName(), databaseDDL, "",
								sourceGlueCatalogId, exportRunId, msgAttrExportBatchId.getValue(), false);
					}
					// Get Tables for a given Database
					List<Table> dbTableList = glueUtil.getTables(glue, sourceGlueCatalogId, database.getName());
					for (Table table : dbTableList) {
						List<Partition> partitionList = glueUtil.getPartitions(glue, sourceGlueCatalogId, table.getDatabaseName(), table.getName());
						if(partitionList.size() <= partitionThreshold) {
							System.out.printf("Database: %s, Table: %s, num_partitions: %d \n", table.getDatabaseName(), table.getName(), partitionList.size());
							TableWithPartitions tableWithParts = new TableWithPartitions();
							tableWithParts.setPartitionList(partitionList);
							tableWithParts.setTable(table);
							
							// Convert Table to JSON String
							String tableDDL = gson.toJson(tableWithParts);
							
							// Publish a message to Amazon SNS topic.
							PublishResult publishTableResponse = snsUtil.publishTableSchemaToSNS(sns, topicArn, table, tableDDL,
									sourceGlueCatalogId, msgAttrExportBatchId.getValue());
							
							Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
							item.put("table_id", new AttributeValue().withS(table.getName().concat("|").concat(table.getDatabaseName())));
							item.put("export_run_id", new AttributeValue().withN(Long.valueOf(exportRunId).toString()));
							item.put("export_batch_id", new AttributeValue().withS(msgAttrExportBatchId.getValue()));
							item.put("source_glue_catalog_id", new AttributeValue().withS(sourceGlueCatalogId));
							item.put("table_schema", new AttributeValue().withS(tableDDL)); 
							item.put("is_large_table", new AttributeValue().withS(Boolean.toString(false)));
							
							if (Optional.ofNullable(publishTableResponse.getMessageId()).isPresent()) {
								item.put("sns_msg_id", new AttributeValue().withS(publishTableResponse.getMessageId()));
								item.put("is_exported", new AttributeValue().withS(Boolean.toString(true)));	
								numberOfTablesExported.getAndIncrement();
							} else {
								item.put("sns_msg_id", new AttributeValue().withS(""));
								item.put("is_exported", new AttributeValue().withS(Boolean.toString(false)));	
							}
							
							itemList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(item)));
						} else {
							LargeTable largeTable = new LargeTable();
							largeTable.setTable(table);
							largeTable.setLargeTable(true);
							largeTable.setNumberOfPartitions(partitionList.size());
							largeTable.setCatalogId(sourceGlueCatalogId);
							
							System.out.printf("Database: %s, Table: %s, num_partitions: %d \n", table.getDatabaseName(), table.getName(), partitionList.size());
							System.out.println("This will be sent to SQS Queue for furhter processing.");
							
							sqsUtil.sendTableSchemaToSQSQueue(sqs, sqsQueue4LargePartTables, largeTable, msgAttrExportBatchId.getValue(), sourceGlueCatalogId);
						}
					}
					System.out.printf("Inserting Table statistics to DynamoDB for database: %s \n", database.getName());
					ddbUtil.insertIntoDynamoDB(itemList, ddbTblNameForTableStatusTracking);
					System.out.printf(
							"Table export statistics: number of tables exist in Database = %d, number of tables exported to SNS = %d. \n",
							dbTableList.size(), numberOfTablesExported.get());
				} else
					System.out.printf(
							"There is no Database with name '%s' exist in Glue Data Catalog. Tables cannot be retrieved. \n",
							database.getName());
			} else {
				System.out.println(
						"Message received from SNS Topic seems to be invalid. It could not be converted to Glue Database Type.");
			}

		}
	}
}