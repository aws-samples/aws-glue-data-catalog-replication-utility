// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.collect.Lists;

/**
 * <p>
 * This is a utility class with methods to write items to DynamoDB table.
 * from / to a DynamoDB table.
 * <p>
 * 
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class DDBUtil {

	/**
	 * Method to track the status of Tables imported 
	 * @param tableStatus
	 * @param sourceGlueCatalogId
	 * @param targetGlueCatalogId
	 * @param importRunId
	 * @param ddbTblName
	 * @return
	 */
	public boolean trackTableImportStatus(TableReplicationStatus tableStatus, String sourceGlueCatalogId,
			String targetGlueCatalogId, long importRunId, String exportBatchId, String ddbTblName) {
		boolean itemInserted = false;
		
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withClientConfiguration(cc).build();
		DynamoDB dynamoDB = new DynamoDB(client);
		
		Table table = dynamoDB.getTable(ddbTblName);
		Item item = new Item().withPrimaryKey("table_id", tableStatus.getTableName().concat("|").concat(tableStatus.getDbName()))
				.withNumber("import_run_id", importRunId)
				.withString("export_batch_id", exportBatchId)
				.withString("table_name", tableStatus.getTableName())
				.withString("database_name", tableStatus.getDbName())
				.withString("table_schema", tableStatus.getTableSchema())
				.withString("target_glue_catalog_id", targetGlueCatalogId)
				.withString("source_glue_catalog_id", sourceGlueCatalogId)
				.withBoolean("table_created", tableStatus.isCreated())
				.withBoolean("table_updated", tableStatus.isUpdated())
				.withBoolean("export_has_partitions", tableStatus.isExportHasPartitions())
				.withBoolean("partitions_updated", tableStatus.isPartitionsReplicated());
		// Write the item to the table
		try {
			PutItemOutcome outcome = table.putItem(item);
			int statusCode = outcome.getPutItemResult().getSdkHttpMetadata().getHttpStatusCode();
			if (statusCode == 200) {
				itemInserted = true;
				System.out
						.println("Table item inserted to DynamoDB table. Table name: " + tableStatus.getTableName());
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("Could not insert a Table import status to DynamoDB table: " + ddbTblName);
		}
		dynamoDB.shutdown();
		return itemInserted;
	}
    
	/**
	 * Method to track the status of Databases imported 
	 * @param sourceGlueCatalogId
	 * @param targetGlueCatalogId
	 * @param ddbTblName
	 * @param databaseName
	 * @param importRunId
	 * @param isCreated
	 * @return
	 */
	public boolean trackDatabaseImportStatus(String sourceGlueCatalogId, String targetGlueCatalogId, String ddbTblName, String databaseName,
			long importRunId, String exportBatchId, boolean isCreated) {
		boolean itemInserted = false;
		
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withClientConfiguration(cc).build();
		DynamoDB dynamoDB = new DynamoDB(client);
		
		com.amazonaws.services.dynamodbv2.document.Table table = dynamoDB.getTable(ddbTblName);
		Item item = new Item().withPrimaryKey("db_id", databaseName).withNumber("import_run_id", importRunId)
				.withString("export_batch_id", exportBatchId).withString("target_glue_catalog_id", targetGlueCatalogId)
				.withString("source_glue_catalog_id", sourceGlueCatalogId).withBoolean("is_created", isCreated);
		// Write the item to the table
		try {
			PutItemOutcome outcome = table.putItem(item);
			int statusCode = outcome.getPutItemResult().getSdkHttpMetadata().getHttpStatusCode();
			if (statusCode == 200) {
				itemInserted = true;
				System.out
						.println("Database item inserted to DynamoDB table. Database name: " + databaseName);
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("Could not insert a Database import status to DynamoDB table: " + ddbTblName);
		}
		dynamoDB.shutdown();
		return itemInserted;
	}
	
	/**
	 * Method to track the status of Tables exported 
	 * @param ddbTblName
	 * @param glueDBName
	 * @param glueTableName
	 * @param glueTableSchema
	 * @param snsMsgId
	 * @param glueCatalogId
	 * @param exportRunId
	 * @param isExported
	 * @return
	 */
	public boolean trackTableExportStatus(String ddbTblName, String glueDBName, String glueTableName,
			String glueTableSchema, String snsMsgId, String glueCatalogId, long exportRunId, String exportBatchId,
			boolean isExported, boolean isLargeTable, String bucketName, String objectKey) {

		boolean itemInserted = false;
		if (Optional.of(glueDBName).isPresent() && Optional.of(glueTableName).isPresent()
				&& Optional.of(glueTableSchema).isPresent() && Optional.of(snsMsgId).isPresent()) {
			
			ClientConfiguration cc = new ClientConfiguration();
			cc.setMaxErrorRetry(10);
			AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withClientConfiguration(cc).build();
			DynamoDB dynamoDB = new DynamoDB(client);
			
			com.amazonaws.services.dynamodbv2.document.Table table = dynamoDB.getTable(ddbTblName);
			
			Item item = new Item().withPrimaryKey("table_id", glueTableName.concat("|").concat(glueDBName))  
					.withNumber("export_run_id", exportRunId).withString("export_batch_id", exportBatchId)
					.withString("source_glue_catalog_id", glueCatalogId).withString("table_schema", glueTableSchema)
					.withString("sns_msg_id", snsMsgId).withBoolean("is_exported", isExported)
					.withBoolean("is_large_table", isLargeTable);
			
			if(Optional.ofNullable(bucketName).isPresent() && Optional.ofNullable(objectKey).isPresent())
				item.withString("s3_bucket_name", bucketName).withString("object_key", objectKey);
		
			// Write the item to the table
			try {
				PutItemOutcome outcome = table.putItem(item);	
				int statusCode = outcome.getPutItemResult().getSdkHttpMetadata().getHttpStatusCode();
				if (statusCode == 200) {
					itemInserted = true;
					System.out.println("Table item inserted to DynamoDB table. Table name: " + glueTableName);
				}
			} catch(Exception e) {
				e.printStackTrace();
				System.out.println("Could not insert a Table export status to DynamoDB table: " + ddbTblName);
			}
			dynamoDB.shutdown();
		} else {
			System.out.println("Not all the values present to insert Table item to ");
		}
		return itemInserted;
	}
	
	/**
	 * Method to track the status of Databases exported 
	 * @param ddbTblName
	 * @param glueDBName
	 * @param glueDBSchema
	 * @param snsMsgId
	 * @param glueCatalogId
	 * @param exportRunId
	 * @param isExported
	 * @return
	 */
	public boolean trackDatabaseExportStatus(String ddbTblName, String glueDBName, String glueDBSchema, String snsMsgId,
			String glueCatalogId, long exportRunId, String exportBatchId, boolean isExported) {
		boolean itemInserted = false;
		
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withClientConfiguration(cc).build();
		DynamoDB dynamoDB = new DynamoDB(client);
		
		com.amazonaws.services.dynamodbv2.document.Table table = dynamoDB.getTable(ddbTblName);
		Item item = new Item().withPrimaryKey("db_id", glueDBName)
				.withNumber("export_run_id", exportRunId)
				.withString("export_batch_id", exportBatchId)
				.withString("source_glue_catalog_id", glueCatalogId)
				.withString("database_schema", glueDBSchema)
				.withString("sns_msg_id", snsMsgId)
				.withBoolean("is_exported", isExported);
		// Write the item to the table
		try {
			PutItemOutcome outcome = table.putItem(item);
			int statusCode = outcome.getPutItemResult().getSdkHttpMetadata().getHttpStatusCode();
			if (statusCode == 200) {
				itemInserted = true;
				System.out.println("Status inserted to DynamoDB table for Glue Database: " + glueDBName);
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("Could not insert a Database export status to DynamoDB table: " + ddbTblName);
		}
		dynamoDB.shutdown();
		return itemInserted;
	}
	
	/**
	 * This method inserts multiple items to a DynamoDB table using Batch Write Item API
	 * @param itemList
	 * @param dynamoDBTblName
	 */
	public void insertIntoDynamoDB(List<WriteRequest> itemList, String dynamoDBTblName) {
		
		System.out.printf("Inserting %d items to DynamoDB using Batch API call. \n", itemList.size());
		AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().build();
		for (List<WriteRequest> miniBatch : Lists.partition(itemList, 25)) { 
			Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
			requestItems.put(dynamoDBTblName, miniBatch);
			BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest()
					.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
					.withRequestItems(requestItems);
			BatchWriteItemResult result = dynamoDB.batchWriteItem(batchWriteItemRequest);
			while (result.getUnprocessedItems().size() > 0) {
				 Map<String, List<WriteRequest>> unprocessedItems = result.getUnprocessedItems();
				 result = dynamoDB.batchWriteItem(unprocessedItems);
			}
		}
		dynamoDB.shutdown();
	}
		
}