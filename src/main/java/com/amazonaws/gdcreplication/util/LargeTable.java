// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.util;

import com.amazonaws.services.glue.model.Table;

/**
 * This is a POJO class for Glue Database Table
 * 
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class LargeTable {

	private String catalogId;
	private boolean largeTable;
	private int numberOfPartitions;
	private Table table;
	private String s3ObjectKey;
	private String s3BucketName;

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public String getS3BucketName() {
		return s3BucketName;
	}

	public void setS3BucketName(String s3BucketName) {
		this.s3BucketName = s3BucketName;
	}

	public String getS3ObjectKey() {
		return s3ObjectKey;
	}

	public void setS3ObjectKey(String s3ObjectKey) {
		this.s3ObjectKey = s3ObjectKey;
	}

	public String getCatalogId() {
		return catalogId;
	}

	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}

	public boolean isLargeTable() {
		return largeTable;
	}

	public void setLargeTable(boolean largeTable) {
		this.largeTable = largeTable;
	}

	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public void setNumberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}
}
