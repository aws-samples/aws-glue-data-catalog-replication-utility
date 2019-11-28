// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.lambda;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.gdcreplication.util.GDCUtil;
import com.amazonaws.gdcreplication.util.LargeTable;
import com.amazonaws.gdcreplication.util.SQSUtil;
import com.amazonaws.gdcreplication.util.TableWithPartitions;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNSRecord;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * This class has AWS Lambda Handler method. Upon invocation, it gets an SNS
 * Event from source SNS Topic, gets the message(s) from the event, parse the
 * message to database or table type and takes one of the following actions on
 * the target glue catalog:
 * 
 * 1. Create a Database if it does not exist already 2. Create a Table if it
 * does not exist already 3. Update a Table if it exist already
 * 
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class ImportDatabaseOrTable implements RequestHandler<SNSEvent, Object> {

	public Object handleRequest(SNSEvent request, Context context) {
		
		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String targetGlueCatalogId = Optional.ofNullable(System.getenv("target_glue_catalog_id")).orElse("1234567890");
		boolean skipTableArchive = Boolean
				.parseBoolean(Optional.ofNullable(System.getenv("skip_archive")).orElse("true"));
		String ddbTblNameForDBStatusTracking = Optional.ofNullable(System.getenv("ddb_name_db_import_status"))
				.orElse("ddb_name_db_import_status");
		String ddbTblNameForTableStatusTracking = Optional.ofNullable(System.getenv("ddb_name_table_import_status"))
				.orElse("ddb_name_table_import_status");
		String sqsQueueURL = Optional.ofNullable(System.getenv("dlq_url_sqs")).orElse("");
		String sqsQueueURLLargeTable = Optional.ofNullable(System.getenv("sqs_queue_url_large_tables")).orElse("");

		// Print environment variables
		printEnvVariables(targetGlueCatalogId, skipTableArchive, ddbTblNameForDBStatusTracking,
				ddbTblNameForTableStatusTracking, sqsQueueURL, region, sqsQueueURLLargeTable);

		// Set client configuration
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);

		// Create Objects for Glue and SQS
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();

		// Process records
		List<SNSRecord> snsRecods = request.getRecords();
		processSNSEvent(context, snsRecods, glue, sqs, sqsQueueURL, sqsQueueURLLargeTable, targetGlueCatalogId,
				ddbTblNameForDBStatusTracking, ddbTblNameForTableStatusTracking, skipTableArchive, region);
		return "Success";
	}

	/**
	 * This method processes SNS event and has the business logic to import
	 * Databases and Tables to Glue Catalog
	 * @param context
	 * @param snsRecods
	 * @param glue
	 * @param sqs
	 * @param sqsQueueURL
	 * @param sqsQueueURLLargeTable
	 * @param targetGlueCatalogId
	 * @param ddbTblNameForDBStatusTracking
	 * @param ddbTblNameForTableStatusTracking
	 * @param skipTableArchive
	 * @param region
	 */
	public void processSNSEvent(Context context, List<SNSRecord> snsRecods, AWSGlue glue, AmazonSQS sqs,
			String sqsQueueURL, String sqsQueueURLLargeTable, String targetGlueCatalogId,
			String ddbTblNameForDBStatusTracking, String ddbTblNameForTableStatusTracking, boolean skipTableArchive,
			String region) {

		SQSUtil sqsUtil = new SQSUtil();
		for (SNSRecord snsRecod : snsRecods) {
			boolean isDatabaseType = false;
			boolean isTableType = false;
			boolean isLargeTable = false;
			LargeTable largeTable = null;
			Database db = null;
			TableWithPartitions table = null;
			Gson gson = new Gson();
			String message = snsRecod.getSNS().getMessage();
			context.getLogger().log("SNS Message Payload: " + message);
			
			// Get message attributes from the SNS Payload
			Map<String, MessageAttribute> msgAttributeMap = snsRecod.getSNS().getMessageAttributes();
			MessageAttribute msgTypeAttr = msgAttributeMap.get("message_type");
			MessageAttribute sourceCatalogIdAttr = msgAttributeMap.get("source_catalog_id");
			MessageAttribute exportBatchIdAttr = msgAttributeMap.get("export_batch_id");
			String sourceGlueCatalogId = sourceCatalogIdAttr.getValue();
			String exportBatchId = exportBatchIdAttr.getValue();
			context.getLogger().log("Message Type: " + msgTypeAttr.getValue());
			context.getLogger().log("Source Catalog Id: " + sourceGlueCatalogId);
			
			// Serialize JSON String based on the message type
			try {
				if (msgTypeAttr.getValue().equalsIgnoreCase("database")) {
					db = gson.fromJson(message, Database.class);
					isDatabaseType = true;
				} else if (msgTypeAttr.getValue().equalsIgnoreCase("table")) {
					table = gson.fromJson(message, TableWithPartitions.class);
					isTableType = true;
				} else if (msgTypeAttr.getValue().equalsIgnoreCase("largeTable")) {
					largeTable = gson.fromJson(message, LargeTable.class);
					isLargeTable = true;
				}
			} catch (JsonSyntaxException e) {
				System.out.println("Cannot parse SNS message to Glue Database Type.");
				e.printStackTrace();
			}
			
			// Execute the business logic based on the message type
			GDCUtil gdcUtil = new GDCUtil();
			if (isDatabaseType) {
				gdcUtil.processDatabseSchema(glue, sqs, targetGlueCatalogId, db, message, sqsQueueURL, sourceGlueCatalogId,
						exportBatchId, ddbTblNameForDBStatusTracking);
			} else if (isTableType) {
				gdcUtil.processTableSchema(glue, sqs, targetGlueCatalogId, sourceGlueCatalogId, table, message,
						ddbTblNameForTableStatusTracking, sqsQueueURL, exportBatchId, skipTableArchive);
			} else if (isLargeTable) {
				sqsUtil.sendLargeTableSchemaToSQS(sqs, sqsQueueURLLargeTable, exportBatchId, sourceGlueCatalogId,
						message, largeTable);
			}
		}
	}

	/**
	 * Print environment variables
	 * @param targetGlueCatalogId
	 * @param ddbTblNameForDBStatusTracking
	 * @param ddbTblNameForTableStatusTracking
	 * @param sqsURL
	 */
	public void printEnvVariables(String targetGlueCatalogId, boolean skipTableArchive,
			String ddbTblNameForDBStatusTracking, String ddbTblNameForTableStatusTracking, String sqsQueueURL,
			String region, String sqsQueueURLLargeTable) {
		System.out.println("Target Catalog Id: " + targetGlueCatalogId);
		System.out.println("Skip Table Archive: " + skipTableArchive);
		System.out.println("DynamoDB Table for DB Import Auditing: " + ddbTblNameForDBStatusTracking);
		System.out.println("DynamoDB Table for Table Import Auditing: " + ddbTblNameForTableStatusTracking);
		System.out.println("Dead Letter Queue URL: " + sqsQueueURL);
		System.out.println("Region: " + region);
		System.out.println("SQS Queue URL for Large Tables: " + sqsQueueURLLargeTable);
	}

	
}
