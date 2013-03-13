/*******************************************************************************
 * Copyright 2013 Johns Hopkins University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package edu.jhu.pha.vospace.process.sax;

import java.util.ArrayList;

public class AsciiTable {
	private int tableId;
	private int columns;
	private String[] columnTypes;
	private String[] columnNames;
	private ArrayList<String[]> rows;
	
	public AsciiTable(int tableId, int columns) {
		this.tableId = tableId;
		this.columns = columns;
		
		rows = new ArrayList<String[]>();
		columnNames = new String[columns];
		columnTypes = new String[columns];
	}

	public int getTableId() {
		return tableId;
	}

	public String[] getColumnTypes() {
		return columnTypes;
	}

	public String[] getColumnNames() {
		return columnNames;
	}
	
	public ArrayList<String[]> getRows() {
		return rows;
	}	
	
	public int getColumns() {
		return columns;
	}
}
