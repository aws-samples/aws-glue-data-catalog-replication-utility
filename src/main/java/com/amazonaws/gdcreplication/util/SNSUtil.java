// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.gson.Gson;

/**
 * This is class has utility methods to work with Amazon SNS
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class SNSUtil {

	
	public PublishResult publishLargeTableSchemaToSNS(AmazonSNS sns, String topicArn, String region, String bucketName, String message,
			String sourceGlueCatalogId, String exportBatchId, String messageType) {
		
		PublishResult publishResponse = null;
		
		PublishRequest publishRequest = new PublishRequest(topicArn, message);
		
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
		messageAttributes.put("source_catalog_id", createStringAttribute(sourceGlueCatalogId));
		messageAttributes.put("message_type", createStringAttribute(messageType));
		messageAttributes.put("export_batch_id", createStringAttribute(exportBatchId));
		messageAttributes.put("bucket_name", createStringAttribute(bucketName));
		messageAttributes.put("region_name", createStringAttribute(region));
		publishRequest.setMessageAttributes(messageAttributes);
		try {
			publishResponse = sns.publish(publishRequest);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Large Table message could not be published to SNS Topic. Topic ARN: " + topicArn);
			System.out.println("Message to be published: " + message);
		}
		return publishResponse;
	}
	
	/**
	 * This method publishes one Table Schema (DDL) to SNS Topic
	 * 
	 * @param sns
	 * @param topicArn
	 * @param databaseDDL
	 * @param sourceGlueCatalogId
	 * @return
	 */
	public PublishResult publishDatabaseSchemaToSNS(AmazonSNS sns, String topicArn, String databaseDDL,
			String sourceGlueCatalogId, String exportBatchId) {
		PublishResult publishResponse = null;
		PublishRequest publishRequest = new PublishRequest(topicArn, databaseDDL);
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
		messageAttributes.put("source_catalog_id", createStringAttribute(sourceGlueCatalogId));
		messageAttributes.put("message_type", createStringAttribute("database"));
		messageAttributes.put("export_batch_id", createStringAttribute(exportBatchId));
		publishRequest.setMessageAttributes(messageAttributes);
		try {
			publishResponse = sns.publish(publishRequest);
		} catch (Exception e) {
			System.out.println("Database schema could not be published to SNS Topic.");
		}
		return publishResponse;
	}

	/**
	 * This method publishes all Database Schemas (DDL) to SNS Topic and tracks the
	 * status in a DynamoDB table.
	 * 
	 * @param sns
	 * @param dBList
	 * @param snsTopicArn
	 * @param ddbUtil
	 * @param ddbTblName
	 * @param sourceGlueCatalogId
	 * @return
	 */
	public int publishDatabaseSchemasToSNS(AmazonSNS sns, List<Database> masterDBList, String snsTopicArn,
			DDBUtil ddbUtil, String ddbTblName, String sourceGlueCatalogId) {
		long exportRunId = System.currentTimeMillis();
		String exportBatchId = Long.toString(exportRunId);
		AtomicInteger numberOfDatabasesExported = new AtomicInteger();
		// Create Message Attributes
		MessageAttributeValue sourceCatalogIdMA = createStringAttribute(sourceGlueCatalogId);
		MessageAttributeValue msgTypeMA = createStringAttribute("database");
		MessageAttributeValue exportBatchIdMA = createStringAttribute(exportBatchId);
		// Convert databases to JSON Messages and publish them to SNS Topic
		for (Database db : masterDBList) {
			// Convert Glue Database to JSON String
			Gson gson = new Gson();
			String databaseDDL = gson.toJson(db);
			// Publish JSON String to Amazon SNS topic
			PublishRequest publishRequest = new PublishRequest(snsTopicArn, databaseDDL);
			Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("source_catalog_id", sourceCatalogIdMA);
			messageAttributes.put("message_type", msgTypeMA);
			messageAttributes.put("export_batch_id", exportBatchIdMA);
			publishRequest.setMessageAttributes(messageAttributes);
			try {
				PublishResult publishResponse = sns.publish(publishRequest);
				numberOfDatabasesExported.getAndIncrement();
				System.out.printf("Schema for Database '%s' published to SNS Topic. Message_Id: %s. \n",
						db.getName(), publishResponse.getMessageId());
				ddbUtil.trackDatabaseExportStatus(ddbTblName, db.getName(), databaseDDL, publishResponse.getMessageId(),
						sourceGlueCatalogId, exportRunId, exportBatchId, true);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.printf(
						"Schema for Database '%s' could not be published to SNS Topic. It will be audited in DynamoDB table. \n",
						db.getName());
				ddbUtil.trackDatabaseExportStatus(ddbTblName, db.getName(), databaseDDL, "", sourceGlueCatalogId,
						exportRunId, exportBatchId, false);
			}
		}
		System.out.println("Number of databases exported to SNS: " + numberOfDatabasesExported.get());
		return numberOfDatabasesExported.get();
	}

	/**
	 * This method publishes Table Schema (DDL) to SNS Topic
	 * 
	 * @param sns
	 * @param topicArn
	 * @param tableDDL
	 * @param sourceGlueCatalogId
	 * @return
	 */
	public PublishResult publishTableSchemaToSNS(AmazonSNS sns, String topicArn, Table table, String tableDDL,
			String sourceGlueCatalogId, String exportBatchId) {
		PublishResult publishResponse = null;
		PublishRequest publishRequest = new PublishRequest(topicArn, tableDDL);
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
		messageAttributes.put("source_catalog_id", createStringAttribute(sourceGlueCatalogId));
		messageAttributes.put("message_type", createStringAttribute("table"));
		messageAttributes.put("export_batch_id", createStringAttribute(exportBatchId));
		publishRequest.setMessageAttributes(messageAttributes);
		try {
			publishResponse = sns.publish(publishRequest);
			System.out.printf("Table schema for Table '%s' of database '%s' published to SNS Topic. Message_Id: %s. \n", table.getName(),
					table.getDatabaseName(), publishResponse.getMessageId());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.printf(
					"Table schema for Table '%s' of database '%s' could not be published to SNS Topic. This will be tracked in DynamoDB table. \n",
					table.getName(), table.getDatabaseName());
		}
		return publishResponse;
	}

	/**
	 * This method creates MessageAttributeValue using a String value
	 * 
	 * @param attributeValue
	 * @return
	 */
	public MessageAttributeValue createStringAttribute(final String attributeValue) {
		final MessageAttributeValue messageAttributeValue = new MessageAttributeValue().withDataType("String")
				.withStringValue(attributeValue);
		return messageAttributeValue;
	}
	
}
