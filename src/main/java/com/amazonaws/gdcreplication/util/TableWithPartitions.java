// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.gdcreplication.util;

import java.util.List;

import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;

public class TableWithPartitions {

	private Table table;
	private List<Partition> partitionList;
	
	public Table getTable() {
		return table;
	}
	public void setTable(Table table) {
		this.table = table;
	}
	public List<Partition> getPartitionList() {
		return partitionList;
	}
	public void setPartitionList(List<Partition> partitionList) {
		this.partitionList = partitionList;
	}
	
	
	
}
