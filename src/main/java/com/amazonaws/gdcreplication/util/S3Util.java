// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class S3Util {

	
	/**
	 *
	 * Create an object in S3 with list of partitions.
	 * @param region
	 * @param bucket
	 * @param objectKey
	 * @param content
	 * @return
	 *
	 * Wrote this method based  on the ideas from Zoran Ivanovic of AWS
	 */
	public boolean createS3Object(String region, String bucket, String objectKey, String content) {
		boolean objectCreated = false;
		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

		byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
		InputStream inputStream = new ByteArrayInputStream(contentBytes);

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentBytes.length);
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, objectKey, inputStream, metadata);
		// send request to S3 to create an object with the content
		try {
			s3.putObject(putObjectRequest);
			objectCreated = true;
			System.out.println("Partition Object uploaded to S3. Object key: " + objectKey);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			inputStream.close();
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while closing InputStream.");
		}
		return objectCreated;
	}

	/**
	 * Upload a file as an object to S3.
	 * @param region
	 * @param bucketName
	 * @param objKeyName
	 * @param localFilePath
	 * @return
	 * @throws IOException
	 */
	public boolean uploadObject(String region, String bucketName, String objKeyName, String localFilePath)
			throws IOException {

		System.out.println("Uploading file to S3.");
		boolean objectUploaded = false;
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();

		try {
			// Upload a text string as a new object.
			s3Client.putObject(bucketName, objKeyName, "Uploaded String Object");
			// Upload a file as a new object with ContentType and title specified.
			PutObjectRequest request = new PutObjectRequest(bucketName, objKeyName, new File(localFilePath));
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentType("plain/text");
			metadata.addUserMetadata("x-amz-meta-title", "PartitionFile");
			request.setMetadata(metadata);
			s3Client.putObject(request);
			objectUploaded = true;
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}

		return objectUploaded;
	}

	public boolean createObject(String region, String bucketName, String tableDDL, String stringObjKeyName)
			throws IOException {

		boolean objectCreated = false;

		try {
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();

			// Upload a text string as a new object.
			s3Client.putObject(bucketName, stringObjKeyName, tableDDL);
			objectCreated = true;

		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
		return objectCreated;
	}

	public void getObject(String region, String bucketName, String key) throws IOException {

		S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
		try {
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(region)
					.withCredentials(new ProfileCredentialsProvider()).build();

			// Get an object and print its contents.
			System.out.println("Downloading an object");
			fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
			System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
			System.out.println("Content: ");
			displayTextInputStream(fullObject.getObjectContent());

			// Get a range of bytes from an object and print the bytes.
			GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key).withRange(0, 9);
			objectPortion = s3Client.getObject(rangeObjectRequest);
			System.out.println("Printing bytes retrieved.");

			displayTextInputStream(objectPortion.getObjectContent());

			// Get an entire object, overriding the specified response headers, and print
			// the object's content.

			ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides().withCacheControl("No-cache")
					.withContentDisposition("attachment; filename=example.txt");
			GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(bucketName, key)
					.withResponseHeaders(headerOverrides);
			headerOverrideObject = s3Client.getObject(getObjectRequestHeaderOverride);
			displayTextInputStream(headerOverrideObject.getObjectContent());
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		} finally {
			// To ensure that the network connection doesn't remain open, close any open
			// input streams.
			if (fullObject != null) {
				fullObject.close();
			}
			if (objectPortion != null) {
				objectPortion.close();
			}
			if (headerOverrideObject != null) {
				headerOverrideObject.close();
			}
		}

	}

	public static void displayTextInputStream(InputStream input) throws IOException {
		// Read the text input stream one line at a time and display each line.
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String line = null;
		while ((line = reader.readLine()) != null) {
			System.out.println(line);
		}
		System.out.println();
	}
	
	public List<Partition> getPartitionsFromS3(String region, String bucket, String key) {

		String contentType = "";
		Gson gson = new Gson();
		S3Object fullObject = null;
		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		System.out.printf("Bucket Name: %s, Object Key: %s \n", bucket, key);
		
		try {
			fullObject = s3.getObject(new GetObjectRequest(bucket, key));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while reading object from S3");	
		}
		
		InputStream input = fullObject.getObjectContent();
		contentType = fullObject.getObjectMetadata().getContentType();
		System.out.println("CONTENT TYPE: " + contentType);

		// Read the text input stream one line at a time and display each line.
		List<Partition> partitionList = new ArrayList<Partition>();

		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				Partition partition = gson.fromJson(line, Partition.class);
				partitionList.add(partition);
			}
		} catch (JsonSyntaxException | IOException e) {
			System.out.println("Exception occured while reading partition information from S3 object.");
			e.printStackTrace();
		}
		System.out.println("Number of partitions read from S3: " + partitionList.size());
		return partitionList;
	}

}
