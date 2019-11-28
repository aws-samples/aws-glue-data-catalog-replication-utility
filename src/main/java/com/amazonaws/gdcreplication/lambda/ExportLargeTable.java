// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.lambda;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.gdcreplication.util.DDBUtil;
import com.amazonaws.gdcreplication.util.GlueUtil;
import com.amazonaws.gdcreplication.util.LargeTable;
import com.amazonaws.gdcreplication.util.S3Util;
import com.amazonaws.gdcreplication.util.SNSUtil;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.gson.Gson;

public class ExportLargeTable implements RequestHandler<SQSEvent, String> {

	@Override
	public String handleRequest(SQSEvent event, Context context) {

		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String topicArn = Optional.ofNullable(System.getenv("sns_topic_arn_export_dbs_tables"))
				.orElse("arn:aws:sns:us-east-1:1234567890:GlueExportSNSTopic");
		String bucketName = Optional.ofNullable(System.getenv("s3_bucket_name")).orElse("");
		String ddbTblNameForTableStatusTracking = Optional.ofNullable(System.getenv("ddb_name_table_export_status"))
				.orElse("ddb_name_table_export_status");
		
		// Set client configuration
		ClientConfiguration cc = new ClientConfiguration();
		cc.setMaxErrorRetry(10);

		// Create Objects for Glue and SQS
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).withClientConfiguration(cc).build();
		AmazonSNS sns = AmazonSNSClientBuilder.standard().withRegion(region).build();
		
		// // Create Objects for Utility classes
		DDBUtil ddbUtil = new DDBUtil();
		GlueUtil glueUtil = new GlueUtil();
		S3Util s3Util = new S3Util();
		SNSUtil snsUtil = new SNSUtil();
		
		String objectKey = "";
		LargeTable largeTable = null;
		boolean recordProcessed = false;
		boolean objectCreated = false;
		
		/**
		 * Iterate and process all the messages which are part of SQSEvent
		 */
		System.out.println("Number of messages in SQS Event: " + event.getRecords().size());
		for (SQSMessage msg : event.getRecords()) {
			String payLoad = new String(msg.getBody());
			String exportBatchId = "";
			String sourceGlueCatalogId = "";
			String messageType = "";

			Gson gson = new Gson();
			long exportRunId = System.currentTimeMillis();

			// Read Message Attributes
			for (Entry<String, MessageAttribute> entry : msg.getMessageAttributes().entrySet()) {
				if ("ExportBatchId".equalsIgnoreCase(entry.getKey())) {
					exportBatchId = entry.getValue().getStringValue();
					System.out.println("Export Batch Id: " + exportBatchId);
				} else if ("SourceGlueDataCatalogId".equalsIgnoreCase(entry.getKey())) {
					sourceGlueCatalogId = entry.getValue().getStringValue();
					System.out.println("Source Glue Data Cagalog Id: " + sourceGlueCatalogId);
				} else if ("SchemaType".equalsIgnoreCase(entry.getKey())) {
					messageType = entry.getValue().getStringValue();
					System.out.println("Message Type " + messageType);
				}
			}
			
			if (messageType.equalsIgnoreCase("largeTable")) {
				largeTable = gson.fromJson(payLoad, LargeTable.class);
				if (largeTable.isLargeTable()) {
					
					// Create object key
					SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
					StringBuilder date = new StringBuilder(simpleDateFormat.format(new Date()));
					objectKey = date.append("_").append(Long.toString(System.currentTimeMillis())).append("_")
							.append(sourceGlueCatalogId).append("_").append(largeTable.getTable().getDatabaseName())
							.append("_").append(largeTable.getTable().getName()).append(".txt").toString();
					
					String content = getPartitionsAndCreateObjectContent(context, glue, glueUtil, sourceGlueCatalogId, largeTable, exportBatchId);
					objectCreated = s3Util.createS3Object(region, bucketName, objectKey, content);
					
				}
				PublishResult publishResponse = null;
				String largeTableJSON = "";
				// Send S3 object key to SNS Topic
				if (objectCreated && !objectKey.equalsIgnoreCase("")) {
					largeTable.setS3ObjectKey(objectKey);
					largeTable.setS3BucketName(bucketName);
					largeTableJSON = gson.toJson(largeTable);
					System.out.println("Large Table JSON: " + largeTableJSON);
					publishResponse = snsUtil.publishLargeTableSchemaToSNS(sns, topicArn, region, bucketName, largeTableJSON,
							sourceGlueCatalogId, exportBatchId, messageType);
					if(Optional.ofNullable(publishResponse).isPresent()) {
						System.out.println("Large Table Schema Published to SNS Topic. Message Id: " + publishResponse.getMessageId());
						recordProcessed = true;
					}
				}
				// track status in DDB
				if (Optional.ofNullable(publishResponse).isPresent()) {
					ddbUtil.trackTableExportStatus(ddbTblNameForTableStatusTracking,
							largeTable.getTable().getDatabaseName(), largeTable.getTable().getName(), largeTableJSON,
							publishResponse.getMessageId(), sourceGlueCatalogId, exportRunId, exportBatchId, true, true,
							bucketName, objectKey);
				} else {
					ddbUtil.trackTableExportStatus(ddbTblNameForTableStatusTracking,
							largeTable.getTable().getDatabaseName(), largeTable.getTable().getName(), largeTableJSON,
							publishResponse.getMessageId(), sourceGlueCatalogId, exportRunId, exportBatchId, false, true,
							null, null);
				}
			}
		}
		if (!recordProcessed) {
			System.out.printf(
					"Schema for table '%s' of database '%s' could not be exported. This is an exception. It will be retried again. \n",
					largeTable.getTable().getName(), largeTable.getTable().getDatabaseName());
			throw new RuntimeException();
		}
		return "Success";
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
	public String getPartitionsAndCreateObjectContent(Context context, AWSGlue glue, GlueUtil glueUtil,
			String sourceGlueCatalogId, LargeTable largeTable, String exportBatchId) {

		StringBuilder sb = new StringBuilder();
		Table table = glueUtil.getTable(glue, sourceGlueCatalogId, largeTable.getTable().getDatabaseName(),
				largeTable.getTable().getName());
		if (Optional.ofNullable(table).isPresent()) {
			List<Partition> partitionList = glueUtil.getPartitions(glue, sourceGlueCatalogId,
					largeTable.getTable().getDatabaseName(), largeTable.getTable().getName());
			AtomicInteger ai = new AtomicInteger();
			for (Partition p : partitionList) {
				Gson gson = new Gson();
				String partitionDDL = gson.toJson(p);
				sb.append(String.format("%s%n", partitionDDL));
				System.out.printf("Partition #: %d, schema: %s. \n", ai.incrementAndGet(), partitionDDL);
			}
		}
		return sb.toString();
	}
}