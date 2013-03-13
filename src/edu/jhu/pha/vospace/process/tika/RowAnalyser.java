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
package edu.jhu.pha.vospace.process.tika;

import java.util.ArrayList;
import java.util.List;

import no.geosoft.cc.util.SmartTokenizer;

public class RowAnalyser {
	private static final int MAX_HEADER_ROWS = 2;
	private char delimiter;
	
	private List<Integer> rows;

	
	public RowAnalyser(char delimiter) {
		rows = new ArrayList<Integer>();
		this.delimiter = delimiter;
	}
	
	public void addRow(String row) {
		rows.add(getRowType(row));
	}
	
	private int getRowType(String row) {
		SmartTokenizer tokenizer = new SmartTokenizer(row,String.valueOf(delimiter));
		int result = DataTypes.UNKNOWN;
		while (tokenizer.hasMoreTokens()) {
			String s = tokenizer.nextToken();
			int type = DataTypes.getDataType(s);
			
			if (result == DataTypes.UNKNOWN) { 
				result = type;
			}
			else if (result != type) {
				return DataTypes.UNKNOWN;
			}			
		}
		return result;
	}
	
	public int getNumHeaderRows() {
		int headerRows = 0;
		int currentRow = 0;
		while (currentRow < rows.size() && currentRow < MAX_HEADER_ROWS) {
			if (rows.get(currentRow) == DataTypes.STRING) {
				headerRows++;
			}
			currentRow++;
		}
		return headerRows;
	}
	
	public int getNumDataRows() {
		return rows.size() - getNumHeaderRows();
	}
	
	public int getNumRows() {
		return rows.size();
	}

	public int getNumStringRows() {
		int n = 0;
		for (int t: rows) {
			if (t == DataTypes.STRING) n++;
		}
		return n;
	}
	
}
