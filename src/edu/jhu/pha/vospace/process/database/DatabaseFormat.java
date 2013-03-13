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

public interface DatabaseFormat {
	
	public String getSingleType();
	public String getDoubleType();
	public String getUInt8Type();
	public String getInt16Type();
	public String getInt32Type();
	public String getInt64Type();
	public String getCharFixedType(int n);
	public String getCharVariableType();
	public String escapeChars(String s);
	public String formatObjectName(String s);
	public String formatCharString(String s);
	public String formatDateTime(String s);
	public String getDatabaseType(String type);	

}
