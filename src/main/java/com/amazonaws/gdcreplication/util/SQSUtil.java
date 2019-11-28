// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.util;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.gson.Gson;

public class SQSUtil {

	public boolean sendTableSchemaToSQSQueue(AmazonSQS sqs, String queueUrl, LargeTable largeTable,
			String exportBatchId, String sourceGlueCatalogId) {

		Gson gson = new Gson();
		String tableInfo = gson.toJson(largeTable);
		System.out.println(tableInfo);
		
		int statusCode = 400;
		boolean messageSentToSQS = false;
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("ExportBatchId",
				new MessageAttributeValue().withDataType("String.ExportBatchId").withStringValue(exportBatchId));
		messageAttributes.put("SourceGlueDataCatalogId", new MessageAttributeValue()
				.withDataType("String.SourceGlueDataCatalogId").withStringValue(sourceGlueCatalogId));
		messageAttributes.put("SchemaType",
				new MessageAttributeValue().withDataType("String.SchemaType").withStringValue("largeTable"));

		SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueUrl)
				.withMessageBody(tableInfo).withMessageAttributes(messageAttributes);

		try {
			SendMessageResult sendMsgRes = sqs.sendMessage(req);
			statusCode = sendMsgRes.getSdkHttpMetadata().getHttpStatusCode();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while writing message to SQS. " + e.getLocalizedMessage());
		}
		if (statusCode == 200) {
			messageSentToSQS = true;
			System.out.printf("Table details for table '%s' of database '%s' sent to SQS. \n",
					largeTable.getTable().getName(), largeTable.getTable().getDatabaseName());
		}
		return messageSentToSQS;

	}

	public void sendLargeTableSchemaToSQS(AmazonSQS sqs, String queueUrl,
			String exportBatchId, String sourceGlueCatalogId, String message, LargeTable largeTable) {

		int statusCode = 400;
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("ExportBatchId",
				new MessageAttributeValue().withDataType("String.ExportBatchId").withStringValue(exportBatchId));
		messageAttributes.put("SourceGlueDataCatalogId", new MessageAttributeValue()
				.withDataType("String.SourceGlueDataCatalogId").withStringValue(sourceGlueCatalogId));
		messageAttributes.put("SchemaType",
				new MessageAttributeValue().withDataType("String.SchemaType").withStringValue("largeTable"));

		SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueUrl)
				.withMessageBody(message).withMessageAttributes(messageAttributes);
		
		try {
			SendMessageResult sendMsgRes = sqs.sendMessage(req);
			statusCode = sendMsgRes.getSdkHttpMetadata().getHttpStatusCode();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while writing message to SQS. " + e.getLocalizedMessage());
		}
		if (statusCode == 200)
			System.out.printf("Large Table schema for table '%s' of database '%s' sent to SQS. \n",
					largeTable.getTable().getName(), largeTable.getTable().getDatabaseName());

	}
	
	public void sendTableSchemaToDeadLetterQueue(AmazonSQS sqs, String queueUrl, TableReplicationStatus tableStatus,
			String exportBatchId, String sourceGlueCatalogId) {

		int statusCode = 400;
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("ExportBatchId",
				new MessageAttributeValue().withDataType("String.ExportBatchId").withStringValue(exportBatchId));
		messageAttributes.put("SourceGlueDataCatalogId", new MessageAttributeValue()
				.withDataType("String.SourceGlueDataCatalogId").withStringValue(sourceGlueCatalogId));
		messageAttributes.put("SchemaType",
				new MessageAttributeValue().withDataType("String.SchemaType").withStringValue("Table"));

		SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueUrl)
				.withMessageBody(tableStatus.getTableSchema()).withMessageAttributes(messageAttributes);

		try {
			SendMessageResult sendMsgRes = sqs.sendMessage(req);
			statusCode = sendMsgRes.getSdkHttpMetadata().getHttpStatusCode();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while writing message to SQS. " + e.getLocalizedMessage());
		}
		if (statusCode == 200)
			System.out.printf("Table schema for table '%s' of database '%s' sent to SQS. \n",
					tableStatus.getTableName(), tableStatus.getDbName());

	}

	public void sendDatabaseSchemaToDeadLetterQueue(AmazonSQS sqs, String queueUrl, String databaseDDL,
			String databaseName, String exportBatchId, String sourceGlueCatalogId) {

		int statusCode = 400;
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("ExportBatchId",
				new MessageAttributeValue().withDataType("String.ExportBatchId").withStringValue(exportBatchId));
		messageAttributes.put("SourceGlueDataCatalogId", new MessageAttributeValue()
				.withDataType("String.SourceGlueDataCatalogId").withStringValue(sourceGlueCatalogId));
		messageAttributes.put("SchemaType",
				new MessageAttributeValue().withDataType("String.SchemaType").withStringValue("Database"));

		SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(databaseDDL)
				.withMessageAttributes(messageAttributes);

		try {
			SendMessageResult sendMsgRes = sqs.sendMessage(req);
			statusCode = sendMsgRes.getSdkHttpMetadata().getHttpStatusCode();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while writing message to SQS. " + e.getLocalizedMessage());
		}
		if (statusCode == 200)
			System.out.printf("Database schema for database '%s' sent to SQS. \n", databaseName);

	}

}
