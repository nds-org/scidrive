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
package edu.jhu.pha.vospace.process.database;

public class SQLServerFormat implements DatabaseFormat {

	private static final String UINT8 = "TINYINT";
	private static final String INT16 = "SMALLINT";
	private static final String INT32 = "INT";
	private static final String INT64 = "BIGINT";
	private static final String SINGLE = "FLOAT(24)";
	private static final String DOUBLE = "FLOAT(53)";
	private static final String CHAR_FIXED = "CHAR(?)";
	private static final String CHAR_VARIABLE = "VARCHAR(256)";
	
	public String escapeChars(String s) {
		if (s == null) return s;
		return s.replaceAll("'","''").replaceAll("\\-", "'+CHAR(45)+'");
	}
	
	public String getDoubleType() {
		return DOUBLE;
	}
	
	public String getSingleType() {
		return SINGLE;
	}
	
	public String getUInt8Type() {
		return UINT8;
	}
	
	public String getInt16Type() {
		return INT16;
	}
	
	public String getInt32Type() {
		return INT32;
	}
	
	public String getInt64Type() {
		return INT64;
	}
	
	public String getCharFixedType(int n) {
		return CHAR_FIXED.replace("?", String.valueOf(n));
	}
	
	public String getCharVariableType() {
		return CHAR_VARIABLE;
	}
	
	public String formatObjectName(String name) {
		if (name == null) return "[]";
		else return "[" + name + "]";
	}
	
	public String formatCharString(String value) {
		if (value == null) return "NULL";
		else return "'" + escapeChars(value) + "'";
	}

	public String getDatabaseType(String type) {
		String databaseFormat;
		char c = type.charAt(0);
		switch (c) {
			case 'B':
				databaseFormat = UINT8;
				break;
			case 'I':
				databaseFormat = INT16;
				break;
			case 'J':
				databaseFormat = INT32;
				break;
			case 'K':
				databaseFormat = INT64;
				break;
			case 'E':
				databaseFormat = SINGLE;
				break;
			case 'D':
				databaseFormat = DOUBLE;
				break;
			case 'A':
				if (type.length()>1) {
					int length = Integer.parseInt(type.substring(1));
					databaseFormat = CHAR_FIXED.replace("?", String.valueOf(length));
				}
				else {
					databaseFormat = CHAR_VARIABLE;
				}
				break;
			default:
				databaseFormat = CHAR_VARIABLE;
				break;
		}	
		
		return databaseFormat;
	}

	public String formatDateTime(String s) {
		if (s == null) return "NULL";
		else return "CASE ISDATE('"+s+"') WHEN 1 THEN CONVERT(SMALLDATETIME,'"+s+"') ELSE NULL END";
	}
}
