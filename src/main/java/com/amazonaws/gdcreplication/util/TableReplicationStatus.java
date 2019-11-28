// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.util;

/**
 * 
 * This is a POJO class for Glue Database Table Replication Status
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
 
public class TableReplicationStatus {

	private String dbName;
	private String tableName;
	private String replicationDay;
	private String tableSchema;
	private long replicationTime;
	private boolean created;
	private boolean updated;
	private boolean replicated;
	private boolean exportHasPartitions;
	private boolean partitionsReplicated;
	private boolean error;
	private boolean dbNotFoundError;
	
	public boolean isDbNotFoundError() {
		return dbNotFoundError;
	}
	public void setDbNotFoundError(boolean dbNotFoundError) {
		this.dbNotFoundError = dbNotFoundError;
	}
	public boolean isError() {
		return error;
	}
	public void setError(boolean error) {
		this.error = error;
	}
	public String getTableSchema() {
		return tableSchema;
	}
	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public boolean isReplicated() {
		return replicated;
	}
	public void setReplicated(boolean replicated) {
		this.replicated = replicated;
	}
	public String getReplicationDay() {
		return replicationDay;
	}
	public void setReplicationDay(String replicationDay) {
		this.replicationDay = replicationDay;
	}
	public long getReplicationTime() {
		return replicationTime;
	}
	public void setReplicationTime(long replicationTime) {
		this.replicationTime = replicationTime;
	}
	public boolean isCreated() {
		return created;
	}
	public void setCreated(boolean created) {
		this.created = created;
	}
	public boolean isUpdated() {
		return updated;
	}
	public void setUpdated(boolean updated) {
		this.updated = updated;
	}
	public boolean isExportHasPartitions() {
		return exportHasPartitions;
	}
	public void setExportHasPartitions(boolean exportHasPartitions) {
		this.exportHasPartitions = exportHasPartitions;
	}
	public boolean isPartitionsReplicated() {
		return partitionsReplicated;
	}
	public void setPartitionsReplicated(boolean partitionsReplicated) {
		this.partitionsReplicated = partitionsReplicated;
	}
	
}
