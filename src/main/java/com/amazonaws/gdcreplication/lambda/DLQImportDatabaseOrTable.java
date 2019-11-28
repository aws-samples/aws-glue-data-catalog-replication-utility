// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.lambda;

import java.util.Map.Entry;
import java.util.Optional;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.gdcreplication.util.GDCUtil;
import com.amazonaws.gdcreplication.util.TableWithPartitions;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class DLQImportDatabaseOrTable implements RequestHandler<SQSEvent, String> {

	@Override
	public String handleRequest(SQSEvent event, Context context) {

		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String targetGlueCatalogId = Optional.ofNullable(System.getenv("target_glue_catalog_id")).orElse("1234567890");
		boolean skipTableArchive = Boolean
				.parseBoolean(Optional.ofNullable(System.getenv("skip_archive")).orElse("true"));
		String ddbTblNameForDBStatusTracking = Optional.ofNullable(System.getenv("ddb_name_db_import_status"))
				.orElse("ddb_name_db_import_status");
		String ddbTblNameForTableStatusTracking = Optional.ofNullable(System.getenv("ddb_name_table_import_status"))
				.orElse("ddb_name_table_import_status");
		String sqsQueueURL = Optional.ofNullable(System.getenv("dlq_url_sqs")).orElse("");

		// Print environment variables
		printEnvVariables(targetGlueCatalogId, skipTableArchive, ddbTblNameForDBStatusTracking,
				ddbTblNameForTableStatusTracking, sqsQueueURL, region);

		// Set client configuration
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);

		// Create Objects for Glue and SQS
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();

		/**
		 * Iterate and process all the messages which are part of SQSEvent
		 */
		System.out.println("Number of messages in SQS Event: " + event.getRecords().size());
		for (SQSMessage msg : event.getRecords()) {
			String ddl = new String(msg.getBody());
			String exportBatchId = "";
			String sourceGlueCatalogId = "";
			String schemaType = "";
			boolean isTable = false;

			// Read Message Attributes
			for (Entry<String, MessageAttribute> entry : msg.getMessageAttributes().entrySet()) {
				if ("ExportBatchId".equalsIgnoreCase(entry.getKey())) {
					exportBatchId = entry.getValue().getStringValue();
					System.out.println("Export Batch Id: " + exportBatchId);
				} else if ("SourceGlueDataCatalogId".equalsIgnoreCase(entry.getKey())) {
					sourceGlueCatalogId = entry.getValue().getStringValue();
					System.out.println("Source Glue Data Cagalog Id: " + sourceGlueCatalogId);
				} else if ("SchemaType".equalsIgnoreCase(entry.getKey())) {
					schemaType = entry.getValue().getStringValue();
					System.out.println("Message Schema Type " + schemaType);
				}
			}
			System.out.println("Schema: " + ddl);
			if (schemaType.equalsIgnoreCase("Table"))
				isTable = true;

			processsRecord(context, glue, sqs, sqsQueueURL, targetGlueCatalogId, ddbTblNameForDBStatusTracking,
					ddbTblNameForTableStatusTracking, ddl, skipTableArchive, exportBatchId, sourceGlueCatalogId,
					isTable);

		}
		return "Success";
	}

	/**
	 * Print environment variables
	 * 
	 * @param targetGlueCatalogId
	 * @param skipTableArchive
	 * @param ddbTblNameForDBStatusTracking
	 * @param ddbTblNameForTableStatusTracking
	 * @param sqsQueueURL
	 * @param region
	 */
	public void printEnvVariables(String targetGlueCatalogId, boolean skipTableArchive,
			String ddbTblNameForDBStatusTracking, String ddbTblNameForTableStatusTracking, String sqsQueueURL,
			String region) {
		System.out.println("Target Catalog Id: " + targetGlueCatalogId);
		System.out.println("Skip Table Archive: " + skipTableArchive);
		System.out.println("DynamoDB Table for DB Import Auditing: " + ddbTblNameForDBStatusTracking);
		System.out.println("DynamoDB Table for Table Import Auditing: " + ddbTblNameForTableStatusTracking);
		System.out.println("Dead Letter Queue URL: " + sqsQueueURL);
		System.out.println("Region: " + region);
	}

	/**
	 * This method processes a record from SQS
	 * 
	 * @param context
	 * @param glue
	 * @param glueUtil
	 * @param ddbUtil
	 * @param targetGlueCatalogId
	 * @param ddbTblNameForDBStatusTracking
	 * @param ddbTblNameForTableStatusTracking
	 * @param message
	 * @param skipTableArchive
	 * @param exportBatchId
	 * @param sourceGlueCatalogId
	 * @param isTable
	 */
	public void processsRecord(Context context, AWSGlue glue, AmazonSQS sqs, String sqsQueueURL,
			String targetGlueCatalogId, String ddbTblNameForDBStatusTracking, String ddbTblNameForTableStatusTracking,
			String message, boolean skipTableArchive, String exportBatchId, String sourceGlueCatalogId,
			boolean isTable) {

		boolean isDatabaseType = false;
		boolean isTableType = false;
		
		Database db = null;
		TableWithPartitions table = null;
		Gson gson = new Gson();
		
		if (isTable) {
			context.getLogger().log("The input message is of type Glue Table.");
			try {
				table = gson.fromJson(message, TableWithPartitions.class);
				isTableType = true;
			} catch (JsonSyntaxException e) {
				System.out.println("Cannot parse SNS message to Glue Table Type.");
				e.printStackTrace();
			}
		} else {
			context.getLogger().log("The input message is of type Glue Database.");
			try {
				db = gson.fromJson(message, Database.class);
				isDatabaseType = true;
			} catch (JsonSyntaxException e) {
				System.out.println("Cannot parse SNS message to Glue Database Type.");
				e.printStackTrace();
			}
		}
		// Execute the business logic based on the message type
		GDCUtil gdcUtil = new GDCUtil();
		if (isDatabaseType) {
			gdcUtil.processDatabseSchema(glue, sqs, targetGlueCatalogId, db, message, sqsQueueURL, sourceGlueCatalogId,
					exportBatchId, ddbTblNameForDBStatusTracking);
		} else if (isTableType) {
			gdcUtil.processTableSchema(glue, sqs, targetGlueCatalogId, sourceGlueCatalogId, table, message,
					ddbTblNameForTableStatusTracking, sqsQueueURL, exportBatchId, skipTableArchive);
		}
	}
}