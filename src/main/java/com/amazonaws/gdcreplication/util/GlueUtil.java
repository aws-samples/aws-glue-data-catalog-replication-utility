// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.gdcreplication.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionResult;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateDatabaseResult;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionResult;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateTableResult;
import com.google.common.collect.Lists;

/**
 * This is class has utility methods to work with AWS Glue Data Catalog
 * 
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class GlueUtil {

	/**
	 * This method checks if a Database exist with the given name in the Glue Data
	 * Catalog
	 * 
	 * @param glue
	 * @param targetCatalogId
	 * @param db
	 * @return
	 */
	public Database getDatabaseIfExist(AWSGlue glue, String targetCatalogId, Database db) {
		Database database = null;
		GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest();
		getDatabaseRequest.setCatalogId(targetCatalogId);
		getDatabaseRequest.setName(db.getName());
		try {
			GetDatabaseResult getDatabaseResult = glue.getDatabase(getDatabaseRequest);
			database = getDatabaseResult.getDatabase();
		} catch (EntityNotFoundException e) {
			System.out.printf("Database '%s' not found. \n", db.getName());
		}
		return database;
	}

	/**
	 * This method get all the databases from a given Glue Data Catalog
	 * 
	 * @param glue
	 * @param sourceGlueCatalogId
	 * @return
	 */
	public List<Database> getDatabases(AWSGlue glue, String sourceGlueCatalogId) {
		List<Database> masterDBList = new ArrayList<Database>();
		GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest();
		getDatabasesRequest.setCatalogId(sourceGlueCatalogId);
		GetDatabasesResult getDatabasesResult = glue.getDatabases(getDatabasesRequest);
		List<Database> databaseList = getDatabasesResult.getDatabaseList();
		masterDBList.addAll(databaseList);
		String databaseResultNextToken = getDatabasesResult.getNextToken();
		if (Optional.ofNullable(databaseResultNextToken).isPresent()) {
			do {
				// create a new GetDatabasesRequest using next token.
				getDatabasesRequest = new GetDatabasesRequest();
				getDatabasesRequest.setNextToken(databaseResultNextToken);
				getDatabasesResult = glue.getDatabases(getDatabasesRequest);
				databaseList = getDatabasesResult.getDatabaseList();
				masterDBList.addAll(databaseList);
				databaseResultNextToken = getDatabasesResult.getNextToken();
			} while (Optional.ofNullable(databaseResultNextToken).isPresent());
		}
		System.out.println("Total number of databases fetched: " + masterDBList.size());
		return masterDBList;
	}

	/**
	 * This method creates a new Database in Glue Data Catalog
	 * 
	 * @param glue
	 * @param targetGlueCatalogId
	 * @param db
	 * @return
	 */

	public DBReplicationStatus createGlueDatabase(AWSGlue glue, String targetGlueCatalogId, String dbName,
			String dbDescription) {
		DBReplicationStatus dbStatus = new DBReplicationStatus();
		CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
		DatabaseInput databaseInput = new DatabaseInput();
		databaseInput.setName(dbName);
		databaseInput.setDescription(dbDescription);
		createDatabaseRequest.setDatabaseInput(databaseInput);
		try {
			CreateDatabaseResult result = glue.createDatabase(createDatabaseRequest);
			int statusCode = result.getSdkHttpMetadata().getHttpStatusCode();
			if (statusCode == 200) {
				System.out.printf("Database created successfully. Database name: '%s'. \n", dbName);
				dbStatus.setCreated(true);
				dbStatus.setError(false);
			} else
				System.out.println("Database could not be created");
		} catch (Exception e) {
			e.printStackTrace();
			dbStatus.setDbName(dbName);
			dbStatus.setError(true);
			System.out.println("Exception thrown while creating Glue Database");
		}
		return dbStatus;
	}

	public DBReplicationStatus createGlueDatabase(AWSGlue glue, String targetGlueCatalogId, Database db) {
		DBReplicationStatus dbStatus = new DBReplicationStatus();
		CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
		DatabaseInput databaseInput = new DatabaseInput();
		databaseInput.setName(db.getName());
		databaseInput.setDescription(db.getDescription());
		databaseInput.setLocationUri(db.getLocationUri());
		databaseInput.setParameters(db.getParameters());
		createDatabaseRequest.setDatabaseInput(databaseInput);
		try {
			CreateDatabaseResult result = glue.createDatabase(createDatabaseRequest);
			int statusCode = result.getSdkHttpMetadata().getHttpStatusCode();
			if (statusCode == 200) {
				System.out.printf("Database created successfully. Database name: '%s'. \n", db.getName());
				dbStatus.setCreated(true);
				dbStatus.setError(false);
			} else
				System.out.println("Database could not be created");
		} catch (Exception e) {
			e.printStackTrace();
			dbStatus.setDbName(db.getName());
			dbStatus.setError(true);
			System.out.printf("Exception in creating Database with name: '%s'. \n", db.getName());
		}
		return dbStatus;
	}

	/**
	 * This method creates a TableInput object using Table object
	 * 
	 * @param table
	 * @return
	 */
	public TableInput createTableInput(Table table) {
		TableInput tableInput = new TableInput();
		tableInput.setDescription(table.getDescription());
		tableInput.setLastAccessTime(table.getLastAccessTime());
		tableInput.setOwner(table.getOwner());
		tableInput.setName(table.getName());
		if (Optional.ofNullable(table.getStorageDescriptor()).isPresent()) {
			tableInput.setStorageDescriptor(table.getStorageDescriptor());
			if (Optional.ofNullable(table.getStorageDescriptor().getParameters()).isPresent())
				tableInput.setParameters(table.getStorageDescriptor().getParameters());
		}
		tableInput.setPartitionKeys(table.getPartitionKeys());
		tableInput.setTableType(table.getTableType());
		tableInput.setViewExpandedText(table.getViewExpandedText());
		tableInput.setViewOriginalText(table.getViewOriginalText());
		tableInput.setParameters(table.getParameters());
		return tableInput;
	}

	/**
	 * This method gets all the tables for a given databases from Glue Data Catalog
	 * 
	 * @param glue
	 * @param glueCatalogId
	 * @param databaseName
	 * @return
	 */
	public List<Table> getTables(AWSGlue glue, String glueCatalogId, String databaseName) {
		System.out.printf("Start - Fetching table list for Database %s \n", databaseName);
		List<Table> masterTableList = new ArrayList<Table>();
		GetTablesRequest getTablesRequest = new GetTablesRequest();
		getTablesRequest.setCatalogId(glueCatalogId);
		getTablesRequest.setDatabaseName(databaseName);
		GetTablesResult getTablesResult = glue.getTables(getTablesRequest);
		List<Table> tableList = getTablesResult.getTableList();
		masterTableList.addAll(tableList);
		String tableResultNextToken = getTablesResult.getNextToken();
		if (Optional.ofNullable(tableResultNextToken).isPresent()) {
			do {
				// creating a new GetTablesResult using next token.
				getTablesRequest = new GetTablesRequest();
				getTablesRequest.setNextToken(tableResultNextToken);
				getTablesRequest.setCatalogId(glueCatalogId);
				getTablesRequest.setDatabaseName(databaseName);
				getTablesResult = glue.getTables(getTablesRequest);
				tableList = getTablesResult.getTableList();
				masterTableList.addAll(tableList);
				tableResultNextToken = getTablesResult.getNextToken();
			} while (Optional.ofNullable(tableResultNextToken).isPresent());
		}
		System.out.printf("Database '%s' has %d tables. \n", databaseName, masterTableList.size());
		System.out.printf("End - Fetching table list for Database %s \n", databaseName);
		return masterTableList;
	}

	/**
	 * This method gets a Table using the given name from Glue Data Catalog. If
	 * there is no table exist with the provided name then it returns null.
	 * 
	 * @param glue
	 * @param glueCatalogId
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	public Table getTable(AWSGlue glue, String glueCatalogId, String databaseName, String tableName) {
		Table table = null;
		GetTableRequest getTableRequest = new GetTableRequest();
		getTableRequest.setDatabaseName(databaseName);
		getTableRequest.setName(tableName);
		getTableRequest.setCatalogId(glueCatalogId);
		try {
			GetTableResult tableResult = glue.getTable(getTableRequest);
			table = tableResult.getTable();
		} catch (EntityNotFoundException e) {
			System.out.printf("Table '%s' not found. \n", tableName);
		}
		return table;
	}

	/**
	 * This method creates or updates a Table in Glue Data Catalog
	 * 
	 * @param glue
	 * @param sourceTable
	 * @param targetGlueCatalogId
	 * @param skipTableArchive
	 * @return
	 */
	public TableReplicationStatus createOrUpdateTable(AWSGlue glue, Table sourceTable, String targetGlueCatalogId,
			boolean skipTableArchive) {

		TableReplicationStatus tableStatus = new TableReplicationStatus();
		tableStatus.setTableName(sourceTable.getName());
		tableStatus.setDbName(sourceTable.getDatabaseName());
		tableStatus.setReplicationTime(System.currentTimeMillis());

		// Check if a table exist already
		GetTableRequest targetTableRequest = new GetTableRequest();
		targetTableRequest.setCatalogId(targetGlueCatalogId);
		targetTableRequest.setDatabaseName(sourceTable.getDatabaseName());
		targetTableRequest.setName(sourceTable.getName());
		Table targetTable = null;
		try {
			GetTableResult targetTableResult = glue.getTable(targetTableRequest);
			targetTable = targetTableResult.getTable();
		} catch (EntityNotFoundException e) {
			System.out.printf("Table '%s' not found. It will be created. \n", sourceTable.getName());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception in getting getTable");
		}
		TableInput tableInput = createTableInput(sourceTable);

		// If table exist - update the table with the schema in the input message.
		if (Optional.ofNullable(targetTable).isPresent()) {
			System.out.println("Table exist. It will be updated");
			UpdateTableRequest updateTableRequest = new UpdateTableRequest();
			updateTableRequest.setTableInput(tableInput);
			updateTableRequest.setSkipArchive(skipTableArchive);
			updateTableRequest.setDatabaseName(sourceTable.getDatabaseName());

			try {
				UpdateTableResult updateTableResult = glue.updateTable(updateTableRequest);
				int statusCode = updateTableResult.getSdkHttpMetadata().getHttpStatusCode();
				if (statusCode == 200) {
					tableStatus.setUpdated(true);
					tableStatus.setReplicated(true);
					tableStatus.setError(false);
					System.out.printf("Table '%s' updated successfully. \n", sourceTable.getName());
				}
			} catch (EntityNotFoundException e) {
				e.printStackTrace();
				System.out.printf("Exception thrown while updating table '%s'. Reason: '%s' do not exist already. \n",
						sourceTable.getName(), sourceTable.getDatabaseName());
				tableStatus.setReplicated(false);
				tableStatus.setDbNotFoundError(true);
				tableStatus.setError(true);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.printf("Exception thrown while updating table '%s'. \n", sourceTable.getName());
				tableStatus.setReplicated(false);
				tableStatus.setError(true);
			}
		}
		// If the table do not exist - create a new table with the schema in the input
		// message.
		else {
			CreateTableRequest createTableRequest = new CreateTableRequest();
			createTableRequest.setCatalogId(targetGlueCatalogId);
			createTableRequest.setDatabaseName(sourceTable.getDatabaseName());
			createTableRequest.setTableInput(tableInput);
			try {
				CreateTableResult createTableResult = glue.createTable(createTableRequest);
				int statusCode = createTableResult.getSdkHttpMetadata().getHttpStatusCode();
				if (statusCode == 200) {
					tableStatus.setCreated(true);
					tableStatus.setReplicated(true);
					tableStatus.setError(false);
					System.out.printf("Table '%s' created successfully. \n", sourceTable.getName());
				}
			} catch (EntityNotFoundException e) {
				e.printStackTrace();
				System.out.printf("Exception thrown while creating table '%s'. Reason: '%s' do not exist already. \n.",
						sourceTable.getName(), sourceTable.getDatabaseName());
				tableStatus.setReplicated(false);
				tableStatus.setDbNotFoundError(true);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.printf("Exception thrown while creating table '%s' \n.", sourceTable.getName());
				tableStatus.setReplicated(false);
				tableStatus.setError(true);
			}
		}
		return tableStatus;
	}

	/**
	 * This method gets a list of partitions for a given table.
	 * 
	 * @param glue
	 * @param catalogId
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	public List<Partition> getPartitions(AWSGlue glue, String catalogId, String databaseName, String tableName) {
		List<Partition> masterPartitionList = new ArrayList<Partition>();
		GetPartitionsRequest getPartitionsRequest = new GetPartitionsRequest();
		getPartitionsRequest.setDatabaseName(databaseName);
		getPartitionsRequest.setCatalogId(catalogId);
		getPartitionsRequest.setTableName(tableName);
		GetPartitionsResult getPartitionResult = glue.getPartitions(getPartitionsRequest);
		List<Partition> partitionList = getPartitionResult.getPartitions();
		masterPartitionList.addAll(partitionList);
		String partitionResultNextToken = getPartitionResult.getNextToken();
		if (Optional.ofNullable(partitionResultNextToken).isPresent()) {
			do {
				// create a new GetPartitionsRequest using next token.
				getPartitionsRequest = new GetPartitionsRequest();
				getPartitionsRequest.setDatabaseName(databaseName);
				getPartitionsRequest.setCatalogId(catalogId);
				getPartitionsRequest.setTableName(tableName);
				getPartitionsRequest.setNextToken(partitionResultNextToken);
				getPartitionResult = glue.getPartitions(getPartitionsRequest);
				partitionList = getPartitionResult.getPartitions();
				masterPartitionList.addAll(partitionList);
				partitionResultNextToken = getPartitionResult.getNextToken();
			} while (Optional.ofNullable(partitionResultNextToken).isPresent());
		}
		return masterPartitionList;
	}

	/**
	 * Add partitions in batch mode
	 * @param glue
	 * @param partitionsToAdd
	 * @param catalogId
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	public boolean addPartitions(AWSGlue glue, List<Partition> partitionsToAdd, String catalogId, String databaseName,
			String tableName) {
		AtomicInteger numPartitionsAdded = new AtomicInteger();
		boolean partitionsAdded = false;
		BatchCreatePartitionRequest batchCreatePartitionRequest = new BatchCreatePartitionRequest();
		batchCreatePartitionRequest.setCatalogId(catalogId);
		batchCreatePartitionRequest.setDatabaseName(databaseName);
		batchCreatePartitionRequest.setTableName(tableName);

		List<PartitionInput> partitionInputList = new ArrayList<PartitionInput>();
		for (Partition p : partitionsToAdd) {
			PartitionInput pi = new PartitionInput();
			StorageDescriptor storageDescriptor = p.getStorageDescriptor();
			pi.setStorageDescriptor(storageDescriptor);
			pi.setValues(p.getValues());
			partitionInputList.add(pi);
		}
		System.out.println("Partition Input List Size: " + partitionInputList.size());
		if(partitionInputList.size() > 100)
			System.out.println("The input has more than 100 partitions, it will be sliced into smaller lists with 100 partitions each.");
		
		List<List<PartitionInput>> listofSmallerLists = Lists.partition(partitionInputList, 100);
		for(List<PartitionInput> partInputList : listofSmallerLists) {
			batchCreatePartitionRequest.setPartitionInputList(partInputList);
			try {
				BatchCreatePartitionResult result = glue.batchCreatePartition(batchCreatePartitionRequest);
				int statusCode = result.getSdkHttpMetadata().getHttpStatusCode();
				List<PartitionError> partErrors = result.getErrors();
				if (statusCode == 200 && partErrors.size() == 0) {
					System.out.printf("%d partitions were added to table '%s' of database '%s'. \n", partInputList.size(),
							tableName, databaseName);
					partitionsAdded = true;
					numPartitionsAdded.getAndAdd(partInputList.size());
					System.out.printf("%d of %d partitions added so far. \n", numPartitionsAdded.get(), partitionInputList.size());
				} else {
					System.out.printf("Not all partitions were added. Status Code: %d, Number of partition errors: %d \n",
							statusCode, partErrors.size());
					for (PartitionError pe : partErrors) {
						System.out.println("Partition Error Message: " + pe.getErrorDetail().getErrorMessage());
						List<String> pv = pe.getPartitionValues();
						for (String v : pv) {
							System.out.println("Partition error value: " + v);
						}
					}
				}
			} catch(Exception e) {
				e.printStackTrace();
				System.out.printf("Exception in adding partitions. \n");
				System.out.printf("%d of %d partitions added so far. \n", numPartitionsAdded.get(), partitionInputList.size());
				// TODO - what to do when there are exceptions here?
			}
		}
		System.out.println("Total partitions added: " + numPartitionsAdded.get());
		return partitionsAdded;
	}

	/** 
	 * Delete a single partition
	 * @param glue
	 * @param catalogId
	 * @param databaseName
	 * @param tableName
	 * @param partition
	 * @return
	 */
	public boolean deletePartition(AWSGlue glue, String catalogId, String databaseName, String tableName,
			Partition partition) {
		boolean partitionDeleted = false;
		DeletePartitionRequest deletePartitionRequest = new DeletePartitionRequest();
		deletePartitionRequest.setCatalogId(catalogId);
		deletePartitionRequest.setDatabaseName(databaseName);
		deletePartitionRequest.setTableName(tableName);
		deletePartitionRequest.setPartitionValues(partition.getValues());

		DeletePartitionResult result = glue.deletePartition(deletePartitionRequest);
		int statusCode = result.getSdkHttpMetadata().getHttpStatusCode();
		if (statusCode == 200) {
			System.out.printf("Partition deleted from table '%s' of database '%s' \n", tableName, databaseName);
			partitionDeleted = true;
		}
		return partitionDeleted;
	}

	/**
	 * Delete partitions using Batch mode
	 * @param glue
	 * @param catalogId
	 * @param databaseName
	 * @param tableName
	 * @param partitionsToDelete
	 * @return
	 */
	public boolean deletePartitions(AWSGlue glue, String catalogId, String databaseName, String tableName,
			List<Partition> partitionsToDelete) {

		boolean partitionsDeleted = false;

		BatchDeletePartitionRequest batchDeletePartitionRequest = new BatchDeletePartitionRequest();
		batchDeletePartitionRequest.setCatalogId(catalogId);
		batchDeletePartitionRequest.setDatabaseName(databaseName);
		batchDeletePartitionRequest.setTableName(tableName);

		// Prepare a List of PartitionValueList
		List<PartitionValueList> listOfPartitionValueList = new ArrayList<PartitionValueList>();

		// For each Partition, get its values, add create a PartitionValueList, and add
		// them to List of PartitionValueList
		for (Partition p : partitionsToDelete) {
			PartitionValueList pvList = new PartitionValueList();
			pvList.setValues(p.getValues());
			listOfPartitionValueList.add(pvList);
		}

		System.out.println("Size of List of PartitionValueList: " + listOfPartitionValueList.size());
		List<List<PartitionValueList>> listofSmallerLists = Lists.partition(listOfPartitionValueList, 25);
		for (List<PartitionValueList> smallerList : listofSmallerLists) {
			// Add List of PartitionValueList to BatchDeletePartitionRequest
			batchDeletePartitionRequest.setPartitionsToDelete(smallerList);
			try {
				BatchDeletePartitionResult batchDeletePartitionResult = glue
						.batchDeletePartition(batchDeletePartitionRequest);
				int statusCode = batchDeletePartitionResult.getSdkHttpMetadata().getHttpStatusCode();
				List<PartitionError> partErrors = batchDeletePartitionResult.getErrors();
				if (statusCode == 200 && partErrors.size() == 0) {
					System.out.printf("%d partitions from table '%s' of database '%s' were deleted. \n",
							smallerList.size(), tableName, databaseName);
					partitionsDeleted = true;
				} else {
					System.out.printf(
							"Not all partitions were deleted. Status Code: %d, Number of partition errors: %d \n",
							statusCode, partErrors.size());

					for (PartitionError pe : partErrors) {
						System.out.println("Partition Error Message: " + pe.getErrorDetail().getErrorMessage());
						List<String> pv = pe.getPartitionValues();
						for (String v : pv) {
							System.out.println("Partition value: " + v);
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Exception in deleting partitions.");
				e.printStackTrace();
			}
		}
		return partitionsDeleted;
	}
}
