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

import java.util.ArrayList;

import org.apache.tika.metadata.Metadata;

import edu.jhu.pha.vospace.process.sax.AsciiTable;

public interface Database {
	
	/**
	 * Performs initial database setup. Creates tables, views, etc.
	 * @throws DatabaseException
	 */
	public void setup() throws DatabaseException;
	
	/**
	 * Populates metadata tables. Creates and populates data tables.
	 * @param metadata		metadata obtained from Tika parser
	 * @param dataTables 	data tables
	 * @throws DatabaseException
	 */
	public void update(Metadata metadata, ArrayList<AsciiTable> dataTables) throws DatabaseException;
	
	/**
	 * Closes the database connection
	 * @throws DatabaseException
	 */
	public void close() throws DatabaseException;

}
