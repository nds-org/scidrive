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

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.codehaus.jackson.JsonNode;

import edu.jhu.pha.vospace.process.sax.AsciiTable;

public class MySQLDB implements Database {

	private final Logger log = Logger.getLogger(this.getClass());
	
	private static final DatabaseFormat databaseFormat = new MySQLFormat();
	
	private static final String CONFIG_FILE_NAME = "mysql.config";
	
	private static final String RESOURCES_TABLE_NAME = "ExtrnResources";
	private static final String KEY_VALUE_TABLE_NAME = "ExtrnKeyValue";
	private static final String IMAGES_TABLE_NAME = "ExtrnImages";
	private static final String RESOURCE_TABLES_TABLE_NAME = "ExtrnResourceTables";
	private static final String TABLE_METADATA_TABLE_NAME = "ExtrnTableMetadata";
	private static final String TABLE_SCHEMA_VIEW_NAME = "ExtrnTableSchema";

	private static final String CREATE_RESOURCES_TABLE =
			"CREATE TABLE IF NOT EXISTS "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)+" ("
			+"`resourceId` INT AUTO_INCREMENT,"
			+"`resourceName` VARCHAR(256),"
			+"`resourcePath` VARCHAR(256),"
			+"`dateCreated` DATETIME,"
			+"`dateProcessed` DATETIME,"
			+"`contentType` VARCHAR(256),"
			+"CONSTRAINT `PK_ExtrnResources` PRIMARY KEY (`resourceId`)) ENGINE = InnoDB";
	private static final String CREATE_KEY_VALUE_TABLE = 
			"CREATE TABLE IF NOT EXISTS "+databaseFormat.formatObjectName(KEY_VALUE_TABLE_NAME)+" ("
			+"`resourceId` INT,"
			+"`keyword` VARCHAR(256),"
			+"`value` VARCHAR(256),"
			+"`comment` VARCHAR(256),"
			+"CONSTRAINT `PK_ExtrnKeyValue` PRIMARY KEY (`resourceId`,`keyword`),"
			+"CONSTRAINT `FK_ExtrnKeyValue_resourceId` FOREIGN KEY (`resourceId`) REFERENCES "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)+"(`resourceId`) ON DELETE CASCADE)";
	private static final String CREATE_IMAGES_TABLE = 
			"CREATE TABLE IF NOT EXISTS "+databaseFormat.formatObjectName(IMAGES_TABLE_NAME)+" ("
			+"`resourceId` INT,"
			+"`nAxis1` INT,"
			+"`nAxis2` INT,"
			+"`bitPix` INT,"
			+"`bZero` FLOAT(53),"
			+"`bScale` FLOAT(53),"
			+"`dateObs` DATETIME,"
			+"`origin` VARCHAR(256),"
			+"`object` VARCHAR(256),"
			+"`telescop` VARCHAR(256),"
			+"`expTime` FLOAT(53),"
			+"`filter` VARCHAR(256),"
			+"`crPix1` FLOAT(53),"
			+"`crPix2` FLOAT(53),"
			+"`crVal1` FLOAT(53),"
			+"`crVal2` FLOAT(53),"
			+"`cType1` VARCHAR(256),"
			+"`cType2` VARCHAR(256),"
			+"`cd1_1` FLOAT(53),"
			+"`cd1_2` FLOAT(53),"
			+"`cd2_1` FLOAT(53),"
			+"`cd2_2` FLOAT(53),"
			+"CONSTRAINT `PK_ExtrnImages` PRIMARY KEY (`resourceId`),"
			+"CONSTRAINT `FK_ExtrnImages_resourceId` FOREIGN KEY (`resourceId`) REFERENCES "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)+"(`resourceId`) ON DELETE CASCADE) ENGINE = InnoDB";
	private static final String CREATE_RESOURCE_TABLES_TABLE =
			"CREATE TABLE IF NOT EXISTS "+databaseFormat.formatObjectName(RESOURCE_TABLES_TABLE_NAME)+" ("
			+"`resourceId` INT,"
			+"`tableId` INT,"
			+"`tableName` VARCHAR(256),"
			+"CONSTRAINT `PK_ExtrnResourceTables` PRIMARY KEY (`resourceId`,`tableId`),"
			+"CONSTRAINT `FK_ExtrnResourceTables_resourceId` FOREIGN KEY (`resourceId`) REFERENCES "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)+" (`resourceId`) ON DELETE CASCADE) ENGINE = InnoDB";
	private static final String CREATE_TABLE_METADATA_TABLE = 
			"CREATE TABLE IF NOT EXISTS "+databaseFormat.formatObjectName(TABLE_METADATA_TABLE_NAME)+" ("
			+"`resourceId` INT,"
			+"`tableId` INT,"
			+"`keyword` VARCHAR(256),"
			+"`value` VARCHAR(256),"
			+"`comment` VARCHAR(256),"
			+"CONSTRAINT `PK_ExtrnTableMetadata` PRIMARY KEY (`resourceId`,`tableId`,`keyword`),"
			+"CONSTRAINT `FK_ExtrnTableMetadata_resourceId_tableId` FOREIGN KEY (`resourceId`,`tableId`) REFERENCES "+databaseFormat.formatObjectName(RESOURCE_TABLES_TABLE_NAME)+"(`resourceId`,`tableId`) ON DELETE CASCADE) ENGINE = InnoDB";
	
	private static final String CREATE_TABLE_SCHEMA_VIEW = 
			"CREATE OR REPLACE VIEW "+databaseFormat.formatObjectName(TABLE_SCHEMA_VIEW_NAME)+
			" AS SELECT resourceId, tableId, column_name, column_type FROM extrnresourcetables JOIN information_schema.columns ON extrnresourcetables.tableName = information_schema.columns.table_name";
	 
	
	private String connectionString;
	private Connection connection; 
	
	public void close() throws DatabaseException {
		try {
			connection.close();
		}
		catch (SQLException e) {}
	}
	
	public MySQLDB(JsonNode credentials) throws DatabaseException {
		this.connectionString = credentials.path("connectionString").getTextValue();
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			this.connection = DriverManager.getConnection(connectionString);
		}
		catch (ClassNotFoundException e) {
			throw new DatabaseException("Database driver not found",e);
		}
		catch (SQLException e) {
			throw new DatabaseException("Could not open database connection",e);			
		}
	}
	
	public MySQLDB() throws DatabaseException {
		Properties config = new Properties();
		
		try {
			config.load(new FileInputStream(CONFIG_FILE_NAME));
			this.connectionString = config.getProperty("connectionString");
		}
		catch (IOException e) {
			throw new DatabaseException("Could not load credentials from file",e);
		}
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			this.connection = DriverManager.getConnection(connectionString);
		}
		catch (ClassNotFoundException e) {
			throw new DatabaseException("Database driver not found",e);
		}
		catch (SQLException e) {
			throw new DatabaseException("Could not open database connection",e);			
		}
	}
	
	public void setup() throws DatabaseException {
		try {
			Statement stmt = connection.createStatement();
			log.debug("Executing query...\n"+CREATE_RESOURCES_TABLE);
			stmt.executeUpdate(CREATE_RESOURCES_TABLE);
			
			log.debug("Executing query...\n"+CREATE_KEY_VALUE_TABLE);
			stmt.executeUpdate(CREATE_KEY_VALUE_TABLE);
			
			log.debug("Executing query...\n"+CREATE_IMAGES_TABLE);
			stmt.executeUpdate(CREATE_IMAGES_TABLE);
			
			log.debug("Executing query...\n"+CREATE_RESOURCE_TABLES_TABLE);
			stmt.executeUpdate(CREATE_RESOURCE_TABLES_TABLE);
			
			log.debug("Executing query...\n"+CREATE_TABLE_METADATA_TABLE);
			stmt.executeUpdate(CREATE_TABLE_METADATA_TABLE);

			log.debug("Executing query...\n"+CREATE_TABLE_SCHEMA_VIEW);
			stmt.executeUpdate(CREATE_TABLE_SCHEMA_VIEW);
		}
		catch (SQLException e) {
			throw new DatabaseException("Initial database setup failed",e);
		}
	}
	
	public void update(Metadata metadata, ArrayList<AsciiTable> tables) throws DatabaseException {
		int resourceId = updateResources(metadata);
		
		updateKeyValue(resourceId,metadata);
		String contentType = metadata.get(TikaCoreProperties.TYPE);
		if ((contentType != null) && contentType.endsWith(":IMAGE")) {
			updateImages(resourceId,metadata);
		}
		updateResourceTables(resourceId,metadata,tables);
		
	}
	
	private int updateResources(Metadata metadata) throws DatabaseException {
		String resourceName = metadata.get(TikaCoreProperties.TITLE);
		String resourcePath = metadata.get(TikaCoreProperties.SOURCE);
		String contentType = metadata.get(TikaCoreProperties.TYPE);
		String dateCreated = metadata.get("section0:DATE"); // FITS file creation date according to FITS specification

		String dateProcessed = metadata.get(TikaCoreProperties.METADATA_DATE);

		String query = 
			"INSERT INTO "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)
			+" (`resourceName`,`resourcePath`,`contentType`,`dateCreated`,`dateProcessed`) SELECT * FROM (SELECT "
			+ "? a1,"
			+ "? a2,"
			+ "? a3,"
			+ databaseFormat.formatDateTime(dateCreated)+" a4,"
			+ databaseFormat.formatDateTime(dateProcessed)+" a5) t";
		log.debug("Executing query...\n"+query.toString());
		
		try {
			PreparedStatement pstmt = connection.prepareStatement(query);
			pstmt.setString(1, resourceName);
			pstmt.setString(2, resourcePath);
			pstmt.setString(3, contentType);
			pstmt.executeUpdate();
			pstmt.close();
			
			query = "SELECT LAST_INSERT_ID()";
			log.debug("Executing query...\n"+query);
			
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			if (rs.next()) {
				int resourceId = rs.getInt(1);
				stmt.close();
				rs.close();
				return resourceId;
			}
			else throw new DatabaseException("Could not retrieve last inserted value");
		}
		catch (SQLException e){
			throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME),e);
		}
	}
	
	private void updateKeyValue(int resourceId, Metadata metadata) throws DatabaseException {
		String query = "INSERT INTO "+databaseFormat.formatObjectName(KEY_VALUE_TABLE_NAME)+" (`resourceId`,`keyword`,`value`,`comment`) VALUES (?,?,?,?)";
		log.debug("Executing query...\n"+query);
		
		try {
			PreparedStatement pstmt = connection.prepareStatement(query);
			for (String name: metadata.names()) {
				if (name.startsWith("section0:")) {
					String keyword = name.substring(name.indexOf(":")+1);
					String value = metadata.getValues(name)[0];
					String comment = " ";
					if (metadata.isMultiValued(name)) {
						comment = metadata.getValues(name)[1];
					}
					
					pstmt.setInt(1, resourceId);
					pstmt.setString(2, keyword);
					pstmt.setString(3, value);
					pstmt.setString(4, comment);
					pstmt.addBatch();
				}
			}	
			pstmt.executeBatch();
			pstmt.close();
		}
		catch (SQLException e) {
			throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(KEY_VALUE_TABLE_NAME),e);
		}
	}
	
	public void updateImages(int resourceId, Metadata metadata) throws DatabaseException {
		Integer nAxis1;
		Integer nAxis2;
		Integer bitPix;
		Double bZero;
		Double bScale;
		String dateObs;
		String origin;
		String object;
		String telescop;
		Double expTime;
		String filter;
		Double crPix1;
		Double crPix2;
		Double crVal1;
		Double crVal2;
		String cType1;
		String cType2;
		Double cd1_1;
		Double cd1_2;
		Double cd2_1;
		Double cd2_2;
		
		String s;
		
		s = metadata.get("section0:NAXIS1");
		nAxis1 = (s==null || s.isEmpty())?null:Integer.parseInt(s);
		
		s = metadata.get("section0:NAXIS2");
		nAxis2 = (s==null || s.isEmpty())?null:Integer.parseInt(s);
		
		s = metadata.get("section0:BITPIX");
		bitPix = (s==null || s.isEmpty())?null:Integer.parseInt(s);
		
		s = metadata.get("section0:BZERO");
		bZero = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:BSCALE");
		bScale = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:DATEOBS"); 
		if (s == null) {
			s = metadata.get("section0:DATE-OBS");
		}
		dateObs = (s==null || s.isEmpty())?null:s;
		
		s = metadata.get("section0:ORIGIN");
		origin = (s==null || s.isEmpty())?null:s;
		
		s = metadata.get("section0:OBJECT");
		object = (s==null || s.isEmpty())?null:s;
		
		s = metadata.get("section0:TELESCOP");
		telescop = (s==null || s.isEmpty())?null:s;
		
		s = metadata.get("section0:EXPTIME");
		expTime = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:FILTER");
		filter = (s==null || s.isEmpty())?null:s;
		
		s = metadata.get("section0:CRPIX1");
		crPix1 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CRPIX2");
		crPix2 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CRVAL1");
		crVal1 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CRVAL2");
		crVal2 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CTYPE1");
		cType1 = (s==null || s.isEmpty())?null:s;
		
		s = metadata.get("section0:CTYPE2");
		cType2 = (s==null || s.isEmpty())?null:s;
		
		s = metadata.get("section0:CD1_1");
		cd1_1 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CD1_2");
		cd1_2 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CD2_1");
		cd2_1 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CD2_2");
		cd2_2 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		String query = "INSERT INTO "+databaseFormat.formatObjectName(IMAGES_TABLE_NAME)
			+" (`resourceId`,`nAxis1`,`nAxis2`,`bitPix`,`bZero`,`bScale`,`dateObs`,`origin`,`object`,`telescop`,`expTime`,`filter`,"
			+"`crPix1`,`crPix2`,`crVal1`,`crVal2`,`cType1`,`cType2`,`cd1_1`,`cd1_2`,`cd2_1`,`cd2_2`) SELECT * FROM (SELECT ? a1,? a2,? a3,? a4,? a5,? a6,"+databaseFormat.formatDateTime(dateObs)+" a7,? a8,? a9,? a10,? a11,? a12,? a13,? a14,? a15,? a16,? a17,? a18,? a19,? a20,? a21,? a22) t";
		log.debug("Executing query...\n"+query.toString());

		try {
			PreparedStatement pstmt = connection.prepareStatement(query);
			pstmt.setInt(1,resourceId);
			pstmt.setObject(2,nAxis1);
			pstmt.setObject(3,nAxis2); 
			pstmt.setObject(4,bitPix);
			pstmt.setObject(5,bZero);
			pstmt.setObject(6,bScale);
			pstmt.setObject(7,origin);
			pstmt.setObject(8,object);
			pstmt.setObject(9,telescop);
			pstmt.setObject(10,expTime);
			pstmt.setObject(11,filter);
			pstmt.setObject(12,crPix1);
			pstmt.setObject(13,crPix2);
			pstmt.setObject(14,crVal1);
			pstmt.setObject(15,crVal2);
			pstmt.setObject(16,cType1);
			pstmt.setObject(17,cType2);
			pstmt.setObject(18,cd1_1);
			pstmt.setObject(19,cd1_2);
			pstmt.setObject(20,cd2_1);
			pstmt.setObject(21,cd2_2);
			
			pstmt.executeUpdate();
			pstmt.close();
		}
		catch (SQLException e) {
			throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(IMAGES_TABLE_NAME),e);
		}
	}
	
	public void updateResourceTables(int resourceId, Metadata metadata, ArrayList<AsciiTable> tables) throws DatabaseException {

		for (AsciiTable table : tables) {
			int tableId = table.getTableId();
			String tableName = "Table_"+resourceId+"_"+tableId;
			
			String updateResourceTablesQuery = 
				"INSERT INTO "+databaseFormat.formatObjectName(RESOURCE_TABLES_TABLE_NAME)
				+" (`resourceId`,`tableId`,`tableName`) VALUES(?,?,?)";
			log.debug("Executing query...\n"+updateResourceTablesQuery);
			
			try {
				PreparedStatement pstmt = connection.prepareStatement(updateResourceTablesQuery);
				pstmt.setInt(1, resourceId);
				pstmt.setInt(2, tableId);
				pstmt.setString(3, tableName);
				pstmt.executeUpdate();
				pstmt.close();
			}
			catch (SQLException e) {
				throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(RESOURCE_TABLES_TABLE_NAME),e);
			}
			
			StringBuilder createTableQuery = new StringBuilder();
			createTableQuery.append("CREATE TABLE ");
			createTableQuery.append(databaseFormat.formatObjectName(tableName));
			createTableQuery.append(" (");
			for (int i=0; i<table.getColumns(); i++) {
				if (i!=0) {
					createTableQuery.append(",");
				}
				createTableQuery.append(databaseFormat.formatObjectName(table.getColumnNames()[i]));
				createTableQuery.append(" ");
				createTableQuery.append(databaseFormat.getDatabaseType(table.getColumnTypes()[i]));
			}
			createTableQuery.append(")");
			log.debug("Executing query...\n"+createTableQuery.toString());
			
			try {
				Statement stmt = connection.createStatement();
				stmt.executeUpdate(createTableQuery.toString());
				stmt.close();
			}
			catch (SQLException e) {
				throw new DatabaseException("Table creation failed "+databaseFormat.formatObjectName(tableName),e);
			}			
			
			StringBuilder insertRowQuery = new StringBuilder();
			insertRowQuery.append("INSERT INTO "+databaseFormat.formatObjectName(tableName)+" ");
			boolean first = true;
			for (String[] row : table.getRows()) {
				
				StringBuilder rowValues = new StringBuilder();
				for (int i=0; i<table.getColumns(); i++) {
					if (i!=0) {
						rowValues.append(",");
					}
					String s = row[i];
					if (table.getColumnTypes()[i].charAt(0) == 'A') {
						s = databaseFormat.formatCharString(s);
					}
					rowValues.append(s);
				}
	
				if (first) {
					insertRowQuery.append("\n  SELECT "); 
					first = false;
				}
				else {
					insertRowQuery.append(" UNION\n  SELECT ");
				}
				insertRowQuery.append(rowValues.toString());
			}
			if (!first) {
				log.debug("Executing query...\n"+insertRowQuery.toString());
				
				try {
					Statement stmt = connection.createStatement();
					stmt.executeUpdate(insertRowQuery.toString());
					stmt.close();
				}
				catch (SQLException e) {
					throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(tableName),e);
				}
			}
			

			String prefix = "section"+tableId+":";
			
			String insertTableMetadataQuery = "INSERT INTO "+databaseFormat.formatObjectName(TABLE_METADATA_TABLE_NAME)+" (`resourceId`,`tableId`,`keyword`,`value`,`comment`) VALUES (?,?,?,?,?)";
			log.debug("Executing query...\n"+insertTableMetadataQuery.toString());
			
			try {
				PreparedStatement pstmt = connection.prepareStatement(insertTableMetadataQuery);
				for (String name: metadata.names()) {
					if (name.startsWith(prefix)) {
						String keyword = name.substring(name.indexOf(":")+1);
						String value = metadata.getValues(name)[0];
						String comment = " ";
						if (metadata.isMultiValued(name)) {
							comment = metadata.getValues(name)[1];
						}
						
						pstmt.setInt(1, resourceId);
						pstmt.setInt(2, tableId);
						pstmt.setString(3, keyword);
						pstmt.setString(4, value);
						pstmt.setString(5, comment);
						pstmt.addBatch();
					}
				}	
				pstmt.executeBatch();
				pstmt.close();
			}
			catch (SQLException e) {
				throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(tableName),e);
			}
		}
	}
}
