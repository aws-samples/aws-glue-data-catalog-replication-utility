// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.lambda;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.gdcreplication.util.DDBUtil;
import com.amazonaws.gdcreplication.util.GlueUtil;
import com.amazonaws.gdcreplication.util.LargeTable;
import com.amazonaws.gdcreplication.util.S3Util;
import com.amazonaws.gdcreplication.util.TableReplicationStatus;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class ImportLargeTable implements RequestHandler<SQSEvent, String> {

	@Override
	public String handleRequest(SQSEvent event, Context context) {

		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String targetGlueCatalogId = Optional.ofNullable(System.getenv("target_glue_catalog_id")).orElse("1234567890");
		boolean skipTableArchive = Boolean
				.parseBoolean(Optional.ofNullable(System.getenv("skip_archive")).orElse("true"));
		String ddbTblNameForTableStatusTracking = Optional.ofNullable(System.getenv("ddb_name_table_import_status"))
				.orElse("ddb_name_table_import_status");
		boolean recordProcessed = false;
		
		// Print environment variables
		printEnvVariables(targetGlueCatalogId, skipTableArchive, ddbTblNameForTableStatusTracking, region);
				
		// Set client configuration
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);

		// Create Objects for Glue and SQS
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();
		
		// Iterate and process all the messages which are part of SQSEvent
		System.out.println("Number of messages in SQS Event: " + event.getRecords().size());
		for (SQSMessage msg : event.getRecords()) {
			String ddl = new String(msg.getBody());
			String exportBatchId = "";
			String schemaType = "";
			String sourceGlueCatalogId = "";
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
			if (schemaType.equalsIgnoreCase("largeTable")) {
				recordProcessed = processsRecord(context, glue, sqs, targetGlueCatalogId, ddbTblNameForTableStatusTracking,
						ddl, skipTableArchive, exportBatchId, sourceGlueCatalogId, region);
			}
			if (!recordProcessed) {
				System.out.printf("Input message '%s' could not be processed. This is an exception. It will be reprocessed again. \n", ddl);
				throw new RuntimeException();
			}
		}
		return "Success";
	}
	
	/**
	 * Print environment variables
	 * 
	 * @param targetGlueCatalogId
	 * @param skipTableArchive
	 * @param ddbTblNameForTableStatusTracking
	 * @param region
	 * @param sqsQueueURL
	 */
	public void printEnvVariables(String targetGlueCatalogId, boolean skipTableArchive,
			String ddbTblNameForTableStatusTracking, String region) {
		System.out.println("Target Catalog Id: " + targetGlueCatalogId);
		System.out.println("Skip Table Archive: " + skipTableArchive);
		System.out.println("DynamoDB Table for Table Import Auditing: " + ddbTblNameForTableStatusTracking);
		System.out.println("Region: " + region);
	}

	/**
	 * This method processes a record from SQS
	 * 
	 * @param context
	 * @param glue
	 * @param sqs
	 * @param sqsQueueURL
	 * @param targetGlueCatalogId
	 * @param ddbTblNameForTableStatusTracking
	 * @param message
	 * @param skipTableArchive
	 * @param exportBatchId
	 * @param sourceGlueCatalogId
	 * @param region
	 */
	public boolean processsRecord(Context context, AWSGlue glue, AmazonSQS sqs,
			String targetGlueCatalogId, String ddbTblNameForTableStatusTracking, String message,
			boolean skipTableArchive, String exportBatchId, String sourceGlueCatalogId, String region) {

		boolean recordProcessed = false;
		Gson gson = new Gson();
		S3Util s3Util = new S3Util();
		DDBUtil ddbUtil = new DDBUtil();
		GlueUtil glueUtil = new GlueUtil();

		LargeTable largeTable = null;
		TableReplicationStatus tableStatus = null;
		long importRunId = System.currentTimeMillis();

		// Parse input message to LargeTable object
		try {
			largeTable = gson.fromJson(message, LargeTable.class);
		} catch (JsonSyntaxException e) {
			System.out.println("Cannot parse SNS message to Glue Table Type.");
			e.printStackTrace();
		}

		// Create or update Table
		if (Optional.ofNullable(largeTable).isPresent()) {
			tableStatus = glueUtil.createOrUpdateTable(glue, largeTable.getTable(), targetGlueCatalogId,
					skipTableArchive);
			tableStatus.setTableSchema(message);
		}

		// Update table partitions
		if (!tableStatus.isError()) {
			// Get partitions from S3
			List<Partition> partitionListFromExport = s3Util.getPartitionsFromS3(region, largeTable.getS3BucketName(),
					largeTable.getS3ObjectKey());

			// Get table partitions from Target Account
			List<Partition> partitionsB4Replication = glueUtil.getPartitions(glue, targetGlueCatalogId,
					largeTable.getTable().getDatabaseName(), largeTable.getTable().getName());
			System.out.println("Number of partitions before replication: " + partitionsB4Replication.size());

			// Add Partitions to the table if the export has Partitions
			if (tableStatus.isReplicated() && partitionListFromExport.size() > 0) {
				tableStatus.setExportHasPartitions(true);
				if (partitionsB4Replication.size() == 0) {
					System.out.println("Adding partitions based on the export.");
					boolean partitionsAdded = glueUtil.addPartitions(glue, partitionListFromExport, targetGlueCatalogId,
							largeTable.getTable().getDatabaseName(), largeTable.getTable().getName());
					if (partitionsAdded) {
						tableStatus.setPartitionsReplicated(true);
						recordProcessed = true;
					}
				} else {
					System.out.println(
							"Target table has partitions. They will be deleted first before adding partitions based on Export.");
					// delete partitions in batch mode
					boolean partitionsDeleted = glueUtil.deletePartitions(glue, targetGlueCatalogId,
							largeTable.getTable().getDatabaseName(), largeTable.getTable().getName(),
							partitionsB4Replication);

					// Enable the below code for debugging purpose. Check number of table partitions after deletion
//					List<Partition> partitionsAfterDeletion = glueUtil.getPartitions(glue, targetGlueCatalogId,
//							largeTable.getTable().getDatabaseName(), largeTable.getTable().getName());
//					System.out.println("Number of partitions after deletion: " + partitionsAfterDeletion.size());

					// add partitions from S3 object
					boolean partitionsAdded = glueUtil.addPartitions(glue, partitionListFromExport, targetGlueCatalogId,
							largeTable.getTable().getDatabaseName(), largeTable.getTable().getName());

					if (partitionsDeleted && partitionsAdded) {
						tableStatus.setPartitionsReplicated(true);
						recordProcessed = true;
					}
					// Enable the below code for debugging purpose. Check number of table partitions after addition
//					List<Partition> partitionsAfterAddition = glueUtil.getPartitions(glue, targetGlueCatalogId,
//							largeTable.getTable().getDatabaseName(), largeTable.getTable().getName());
//					System.out.println("Number of partitions after addition: " + partitionsAfterAddition.size());
				}

			} else if (tableStatus.isReplicated() && partitionListFromExport.size() == 0) {
				tableStatus.setExportHasPartitions(false);
				if (partitionsB4Replication.size() > 0) {
					// Export has no partitions but table already has some partitions. Those
					// partitions will be deleted in batch mode.
					boolean partitionsDeleted = glueUtil.deletePartitions(glue, targetGlueCatalogId,
							largeTable.getTable().getDatabaseName(), largeTable.getTable().getName(),
							partitionsB4Replication);
					if (partitionsDeleted) {
						tableStatus.setPartitionsReplicated(true);
						recordProcessed = true;
					}
				}
			}
		}
		// If there is any error in creating/updating table then send it to DLQ
		else {
			System.out.println("Table replicated but partitions were not replicated. Message will be reprocessed again.");
		}

		// Track status in DynamoDB
		ddbUtil.trackTableImportStatus(tableStatus, sourceGlueCatalogId, targetGlueCatalogId, importRunId,
				exportBatchId, ddbTblNameForTableStatusTracking);
		System.out.printf(
				"Processing of Table shcema completed. Result: Table replicated: %b, Export has partitions: %b, "
						+ "Partitions replicated: %b, error: %b \n",
				tableStatus.isReplicated(), tableStatus.isExportHasPartitions(), tableStatus.isPartitionsReplicated(),
				tableStatus.isError());
		
		return recordProcessed;
	}
}