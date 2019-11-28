// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.util;

import java.util.List;
import java.util.Optional;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.sqs.AmazonSQS;

public class GDCUtil {

	/**
	 * This method processes a Message that belongs to Table schema
	 * @param glue
	 * @param sqs
	 * @param targetGlueCatalogId
	 * @param sourceGlueCatalogId
	 * @param tableWithPartitions
	 * @param message
	 * @param ddbTblNameForTableStatusTracking
	 * @param sqsQueueURL
	 * @param exportBatchId
	 * @param skipTableArchive
	 */
	public void processTableSchema(AWSGlue glue, AmazonSQS sqs, String targetGlueCatalogId, String sourceGlueCatalogId,
			TableWithPartitions tableWithPartitions, String message, String ddbTblNameForTableStatusTracking,
			String sqsQueueURL, String exportBatchId, boolean skipTableArchive) {

		DDBUtil ddbUtil = new DDBUtil();
		SQSUtil sqsUtil = new SQSUtil();
		GlueUtil glueUtil = new GlueUtil();
		long importRunId = System.currentTimeMillis();

		// Get Table and its Partitions from Input JSON
		Table table = tableWithPartitions.getTable();
		List<Partition> partitionListFromExport = tableWithPartitions.getPartitionList();

		// Create or update table
		TableReplicationStatus tableStatus = glueUtil.createOrUpdateTable(glue, table, targetGlueCatalogId,
				skipTableArchive);
		// If database not found then create one
		if (tableStatus.isDbNotFoundError()) {
			System.out.printf("Creating Database with name: '%s'. \n", table.getDatabaseName());
			DBReplicationStatus dbStatus = glueUtil.createGlueDatabase(glue, targetGlueCatalogId,
					table.getDatabaseName(),
					"Database Imported from Glue Data Catalog of AWS Account Id: ".concat(sourceGlueCatalogId));
			// Now, try to create / update table again.
			if (dbStatus.isCreated()) {
				tableStatus = glueUtil.createOrUpdateTable(glue, tableWithPartitions.getTable(), targetGlueCatalogId,
						skipTableArchive);
			}
		}
		tableStatus.setTableSchema(message);

		// Update table partitions
		if (!tableStatus.isError()) {
			// Get table partitions from Target Account
			List<Partition> partitionsB4Replication = glueUtil.getPartitions(glue, targetGlueCatalogId,
					table.getDatabaseName(), table.getName());
			System.out.println("Number of partitions before replication: " + partitionsB4Replication.size());

			// Add Partitions to the table if the export has Partitions
			if (partitionListFromExport.size() > 0) {
				tableStatus.setExportHasPartitions(true);
				if (partitionsB4Replication.size() == 0) {
					System.out.println("Adding partitions based on the export.");
					boolean partitionsAdded = glueUtil.addPartitions(glue, partitionListFromExport, targetGlueCatalogId,
							table.getDatabaseName(), table.getName());
					if (partitionsAdded)
						tableStatus.setPartitionsReplicated(true);
				} else {
					System.out.println(
							"Table has partitions. They will be deleted first before adding partitions based on Export.");
					// delete partitions in batch mode
					boolean partitionsDeleted = glueUtil.deletePartitions(glue, targetGlueCatalogId,
							table.getDatabaseName(), table.getName(), partitionsB4Replication);

					// Enable the below code for debugging purpose. Check number of table partitions after deletion
//					List<Partition> partitionsAfterDeletion = glueUtil.getPartitions(glue, targetGlueCatalogId,
//							table.getDatabaseName(), table.getName());
//					System.out.println("Number of partitions after deletion: " + partitionsAfterDeletion.size());

					// add partitions from S3 object
					boolean partitionsAdded = glueUtil.addPartitions(glue, partitionListFromExport, targetGlueCatalogId,
							table.getDatabaseName(), table.getName());

					if (partitionsDeleted && partitionsAdded)
						tableStatus.setPartitionsReplicated(true);

					// Enable the below code for debugging purpose. Check number of table partitions after addition
//					List<Partition> partitionsAfterAddition = glueUtil.getPartitions(glue, targetGlueCatalogId,
//							table.getDatabaseName(), table.getName());
//					System.out.println("Number of partitions after addition: " + partitionsAfterAddition.size());
				}
			} else if (partitionListFromExport.size() == 0) {
				tableStatus.setExportHasPartitions(false);
				if (partitionsB4Replication.size() > 0) {
					// Export has no partitions but table already has some partitions. Those
					// partitions will be deleted in batch mode.
					boolean partitionsDeleted = glueUtil.deletePartitions(glue, targetGlueCatalogId,
							table.getDatabaseName(), table.getName(), partitionsB4Replication);
					if (partitionsDeleted)
						tableStatus.setPartitionsReplicated(true);
				}
			}
		}
		// If there is any error in creating/updating table then send it to DLQ
		else {
			System.out.println("Error in creating/updating table in the Glue Data Catalog. It will be send to DLQ.");
			sqsUtil.sendTableSchemaToDeadLetterQueue(sqs, sqsQueueURL, tableStatus, exportBatchId, sourceGlueCatalogId);
		}
		// Track status in DynamoDB
		ddbUtil.trackTableImportStatus(tableStatus, sourceGlueCatalogId, targetGlueCatalogId, importRunId,
				exportBatchId, ddbTblNameForTableStatusTracking);
		System.out.printf(
				"Processing of Table shcema completed. Result: Table replicated: %b, Export has partitions: %b, "
						+ "Partitions replicated: %b, error: %b \n",
				tableStatus.isReplicated(), tableStatus.isExportHasPartitions(), tableStatus.isPartitionsReplicated(),
				tableStatus.isError());
	}

	/**
	 * This method processes a Message that belongs to Database schema
	 * @param glue
	 * @param sqs
	 * @param targetGlueCatalogId
	 * @param db
	 * @param message
	 * @param sqsQueueURL
	 * @param sourceGlueCatalogId
	 * @param exportBatchId
	 * @param ddbTblNameForDBStatusTracking
	 */
	public void processDatabseSchema(AWSGlue glue, AmazonSQS sqs, String targetGlueCatalogId, Database db,
			String message, String sqsQueueURL, String sourceGlueCatalogId, String exportBatchId,
			String ddbTblNameForDBStatusTracking) {

		DDBUtil ddbUtil = new DDBUtil();
		GlueUtil glueUtil = new GlueUtil();
		SQSUtil sqsUtil = new SQSUtil();

		boolean isDBCreated = false;
		long importRunId = System.currentTimeMillis();
		Database database = glueUtil.getDatabaseIfExist(glue, targetGlueCatalogId, db);
		boolean dbExist = Optional.ofNullable(database).isPresent();
		if (!dbExist) {
			DBReplicationStatus dbStatus = glueUtil.createGlueDatabase(glue, targetGlueCatalogId, db);
			if (dbStatus.isError()) {
				System.out.println("Error in creating database in the Glue Data Catalog. It will be send to DLQ.");
				sqsUtil.sendDatabaseSchemaToDeadLetterQueue(sqs, sqsQueueURL, message, db.getName(), exportBatchId,
						sourceGlueCatalogId);
			} else
				isDBCreated = true;
		} else
			System.out.printf(
					"Database with name '%s' exist already in target Glue Data Catalog. No action will be taken. \n",
					database.getName());
		// Track status in DynamoDB
		ddbUtil.trackDatabaseImportStatus(sourceGlueCatalogId, targetGlueCatalogId, ddbTblNameForDBStatusTracking,
				db.getName(), importRunId, exportBatchId, isDBCreated);
		System.out.printf("Processing of Database shcema completed. Result: DB already exist: %b, DB created: %b. \n",
				dbExist, isDBCreated);
	}
}
