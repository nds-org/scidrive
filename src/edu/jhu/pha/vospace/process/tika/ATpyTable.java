package edu.jhu.pha.vospace.process.tika;

import java.util.ArrayList;
import java.util.List;

public class ATpyTable {
	private ATpyType[] columnTypes;
	private String[] columnNames;
	private ArrayList<String[]> data;
	private int columns;
	
	public ATpyTable(int n) throws Exception {
		if (n > 0) {
			columns = n;
			columnTypes = new ATpyType[n];
			columnNames = new String[n];
			data = new ArrayList<String[]>();
		}
		else {
			throw new Exception("Number of columns must be > 0");
		}
	}
	
	public int getColumns() {
		return columns;
	}
	
	public String getColumnName(int i) {
		return columnNames[i];
	}
	
	public List<String[]> getRows() {
		return data;
	}
	
	public ATpyType getColumnType(int i) {
		return columnTypes[i];
	}
	
	public void setColumnType(int i, ATpyType type) throws Exception {
		if (i < 0 || i >= columns) {
			throw new Exception("Incorrect column index: "+i);
		}
		columnTypes[i] = type;
	}
	
	public void setColumnName(int i, String name) throws Exception {
		if (i < 0 || i >= columns) {
			throw new Exception("Incorrect column index: "+i);
		}
		columnNames[i] = name;
	}
	
	public void setAllColumnTypes(ATpyType[] columnTypes) {
		this.columnTypes = columnTypes;
	}
	
	public void setAllColumnNames(String[] columnNames) {
		this.columnNames = columnNames;
	}
	
	public void addRow(String[] row) throws Exception {
		if (row == null) {
			throw new Exception("Row data cannot be null");
		}
		if (row.length == columns) {
			data.add(row);
		}
		else {
			throw new Exception("Incorrect number of columns: "+row.length);
		}
	}
	
	public String getStringValue(int row, int column) {
		return data.get(row)[column];
	}
	
	public void setStringValue(int row, int column, String value) {
		data.get(row)[column] = value;
	}
	
	public String toString(String delimiter) {
		StringBuilder s = new StringBuilder();
		String header = "";
		for (int i=0; i<columns; i++) {
			if (!"".equals(header)) {
				header += delimiter;
			}
			String kind = "";
				switch (columnTypes[i].getKind()) {
				case ATpyType.INT: kind = "i"; break;
				case ATpyType.FLOAT: kind = "f"; break;
				case ATpyType.STRING: kind = "a"; break;
				default: break;
			}
				
			header += columnNames[i];
			if (!"".equals(kind)) { 
				header += "["+kind+columnTypes[i].getItemSize()+"]";
			}
		}
		s.append(header+"\n");
		
		for (int i=0; i<data.size(); i++) {
			String row = "";
			for (int j=0; j<columns; j++) {
				String value = getStringValue(i,j);
				if (getColumnType(j).getKind() == ATpyType.STRING) {
					value = "\""+ value.replaceAll("\"", "\\x034")+"\"";
				}
				if (!"".equals(row)) {
					row += delimiter;
				}
				row += value;
			}
			s.append(row+"\n");
		}
		return s.toString();
	}
	
	public String toString() {
		return toString(",");
	}
}
