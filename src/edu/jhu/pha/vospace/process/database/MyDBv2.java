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
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.axis2.AxisFault;
import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.codehaus.jackson.JsonNode;

import edu.jhu.pha.cas.services.JobsStub;
import edu.jhu.pha.cas.services.JobsStub.ExecuteQuickJob;
import edu.jhu.pha.cas.services.JobsStub.ExecuteQuickJobResponse;
import edu.jhu.pha.cas.services.CasRestClient;
import edu.jhu.pha.vospace.keystone.KeystoneAuthenticator;
import edu.jhu.pha.vospace.process.sax.AsciiTable;

public class MyDBv2 implements Database {

	private String endpoint = null;
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private static final DatabaseFormat databaseFormat = new SQLServerFormat();
	
	private static final String CONFIG_FILE_NAME = "casjobs.config";

	private static final String RESOURCES_TABLE_NAME = "ExtrnResources";
	private static final String KEY_VALUE_TABLE_NAME = "ExtrnKeyValue";
	private static final String IMAGES_TABLE_NAME = "ExtrnImages";
	private static final String RESOURCE_TABLES_TABLE_NAME = "ExtrnResourceTables";
	private static final String TABLE_METADATA_TABLE_NAME = "ExtrnTableMetadata";
	private static final String TABLE_SCHEMA_VIEW_NAME = "ExtrnTableSchema";

	private static final String CREATE_RESOURCES_TABLE =
			"IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=object_id(N'"+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)+"')"
			+" AND OBJECTPROPERTY(id, N'IsUserTable') = 1)"
			+" CREATE TABLE "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)+" ("
			+"[resourceId] INT IDENTITY,"
			+"[resourceName] VARCHAR(256),"
			+"[resourcePath] VARCHAR(256),"
			+"[dateCreated] SMALLDATETIME,"
			+"[dateProcessed] SMALLDATETIME,"
			+"[contentType] VARCHAR(256),"
			+"CONSTRAINT [PK_ExtrnResources] PRIMARY KEY ([resourceId]))";
	private static final String CREATE_KEY_VALUE_TABLE = 
			"IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=object_id(N'"+databaseFormat.formatObjectName(KEY_VALUE_TABLE_NAME)+"')"
			+" AND OBJECTPROPERTY(id, N'IsUserTable') = 1)"
			+" CREATE TABLE "+databaseFormat.formatObjectName(KEY_VALUE_TABLE_NAME)+" ("
			+"[resourceId] INT,"
			+"[keyword] VARCHAR(256),"
			+"[value] VARCHAR(256),"
			+"[comment] VARCHAR(256),"
			+"CONSTRAINT [PK_ExtrnKeyValue] PRIMARY KEY ([resourceId],[keyword]),"
			+"CONSTRAINT [FK_ExtrnKeyValue_resourceId] FOREIGN KEY ([resourceId]) REFERENCES "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)+"([resourceId]) ON DELETE CASCADE)";
	private static final String CREATE_IMAGES_TABLE = 
			"IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=object_id(N'"+databaseFormat.formatObjectName(IMAGES_TABLE_NAME)+"')"
			+" AND OBJECTPROPERTY(id, N'IsUserTable') = 1)"
			+" CREATE TABLE "+databaseFormat.formatObjectName(IMAGES_TABLE_NAME)+" ("
			+"[resourceId] INT,"
			+"[nAxis1] INT,"
			+"[nAxis2] INT,"
			+"[bitPix] INT,"
			+"[bZero] FLOAT(53),"
			+"[bScale] FLOAT(53),"
			+"[dateObs] SMALLDATETIME,"
			+"[origin] VARCHAR(256),"
			+"[object] VARCHAR(256),"
			+"[telescop] VARCHAR(256),"
			+"[expTime] FLOAT(53),"
			+"[filter] VARCHAR(256),"
			+"[crPix1] FLOAT(53),"
			+"[crPix2] FLOAT(53),"
			+"[crVal1] FLOAT(53),"
			+"[crVal2] FLOAT(53),"
			+"[cType1] VARCHAR(256),"
			+"[cType2] VARCHAR(256),"
			+"[cd1_1] FLOAT(53),"
			+"[cd1_2] FLOAT(53),"
			+"[cd2_1] FLOAT(53),"
			+"[cd2_2] FLOAT(53),"
			+"CONSTRAINT [PK_ExtrnImages] PRIMARY KEY ([resourceId]),"
			+"CONSTRAINT [FK_ExtrnImages_resourceId] FOREIGN KEY ([resourceId]) REFERENCES "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)+"([resourceId]) ON DELETE CASCADE)";
	private static final String CREATE_RESOURCE_TABLES_TABLE =
			"IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=object_id(N'"+databaseFormat.formatObjectName(RESOURCE_TABLES_TABLE_NAME)+"')"
			+" AND OBJECTPROPERTY(id, N'IsUserTable') = 1)"
			+" CREATE TABLE "+databaseFormat.formatObjectName(RESOURCE_TABLES_TABLE_NAME)+" ("
			+"[resourceId] INT,"
			+"[tableId] INT,"
			+"[tableName] VARCHAR(256),"
			+"CONSTRAINT [PK_ExtrnResourceTables] PRIMARY KEY ([resourceId],[tableId]),"
			+"CONSTRAINT [FK_ExtrnResourceTables_resourceId] FOREIGN KEY ([resourceId]) REFERENCES "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)+" ([resourceId]) ON DELETE CASCADE)";
	private static final String CREATE_TABLE_METADATA_TABLE = 
			"IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=object_id(N'"+databaseFormat.formatObjectName(TABLE_METADATA_TABLE_NAME)+"')"
			+" AND OBJECTPROPERTY(id, N'IsUserTable') = 1)"
			+" CREATE TABLE "+databaseFormat.formatObjectName(TABLE_METADATA_TABLE_NAME)+" ("
			+"[resourceId] INT,"
			+"[tableId] INT,"
			+"[keyword] VARCHAR(256),"
			+"[value] VARCHAR(256),"
			+"[comment] VARCHAR(256),"
			+"CONSTRAINT [PK_ExtrnTableMetadata] PRIMARY KEY ([resourceId],[tableId],[keyword]),"
			+"CONSTRAINT [FK_ExtrnTableMetadata_resourceId_tableId] FOREIGN KEY ([resourceId],[tableId]) REFERENCES "+databaseFormat.formatObjectName(RESOURCE_TABLES_TABLE_NAME)+"([resourceId],[tableId]) ON DELETE CASCADE)";
	private static final String CREATE_TABLE_SCHEMA_VIEW = 
			"IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=object_id(N'"+databaseFormat.formatObjectName(TABLE_SCHEMA_VIEW_NAME)+"')"
			+" AND OBJECTPROPERTY(id, N'IsView') = 1)"
			+" EXEC('CREATE VIEW "+databaseFormat.formatObjectName(TABLE_SCHEMA_VIEW_NAME)+" AS"
			+" SELECT [resourceId],[tableId],[syscolumns].[name] [columnName], [systypes].[name] [columnType], [syscolumns].[length]"
			+" FROM "+databaseFormat.formatObjectName(RESOURCE_TABLES_TABLE_NAME) 
			+" JOIN [sysobjects] ON [tableName] = [sysobjects].[name]"
			+" JOIN [syscolumns] ON [sysobjects].[id] = [syscolumns].[id]"
			+" JOIN [systypes] ON [syscolumns].[xtype] = [systypes].[xtype]"
			+" WHERE [sysobjects].[xtype] =''U''')";

	//private JobsStub stub;
	//private ExecuteQuickJob quickJob;
	//private long wsid;
	//private String password;
	CasRestClient client;
	String authToken;
	
	public void close() throws DatabaseException {};
	
	public MyDBv2(JsonNode credentials) throws DatabaseException {
		
		//this.wsid = Long.parseLong(credentials.path("wsid").getTextValue());
		//this.password = credentials.path("password").getTextValue();
		this.endpoint = credentials.path("endpoint").getTextValue();
		/*
		try {
			this.stub = new JobsStub(endpoint+"/services/jobs.asmx");
		}
		catch (AxisFault e) {
			throw new DatabaseException("Could not instantiate CasJobs service stub",e);
		}
		
		quickJob = new ExecuteQuickJob();
		quickJob.setWsid(wsid);
		quickJob.setPw(password);
		quickJob.setContext("MyDB");
		quickJob.setTaskname("my query");
		quickJob.setIsSystem(false);
		*/
		
		this.client = new CasRestClient(endpoint+"/RestApi");
		this.authToken = KeystoneAuthenticator.getAdminToken();
	}
	
	public MyDBv2() throws DatabaseException {
		/*
		Properties config = new Properties();
		
		try {
			config.load(new FileInputStream(CONFIG_FILE_NAME));
		}
		catch (IOException e) {
			throw new DatabaseException("Could not load CasJobs credentials from file",e);
		}
		
		wsid = Long.parseLong(config.getProperty("wsid"));
		password = config.getProperty("password");
		
		try {
			stub = new JobsStub(config.getProperty("jobs_location"));
		}
		catch (AxisFault e) {
			throw new DatabaseException("Could not instantiate CasJobs service stub",e);
		}
		
		quickJob = new ExecuteQuickJob();
		quickJob.setWsid(wsid);
		quickJob.setPw(password);
		quickJob.setContext("MyDB");
		quickJob.setTaskname("my query");
		quickJob.setIsSystem(false);
		*/
	}
	
	public void setup() throws DatabaseException {
		// does nothing
	}
	
	public void setup(Metadata metadata) throws DatabaseException {
		try {
			String keystoneUserId = metadata.get("owner");
			
			log.debug("Executing query...\n"+CREATE_RESOURCES_TABLE);
			//quickJob.setQry(CREATE_RESOURCES_TABLE);
			//stub.executeQuickJob(quickJob);
			client.executeQuickQuery(authToken, keystoneUserId, CREATE_RESOURCES_TABLE);
			
			log.debug("Executing query...\n"+CREATE_KEY_VALUE_TABLE);
			//quickJob.setQry(CREATE_KEY_VALUE_TABLE);
			//stub.executeQuickJob(quickJob);
			client.executeQuickQuery(authToken, keystoneUserId, CREATE_KEY_VALUE_TABLE);
			
			log.debug("Executing query...\n"+CREATE_IMAGES_TABLE);
			//quickJob.setQry(CREATE_IMAGES_TABLE);
			//stub.executeQuickJob(quickJob);
			client.executeQuickQuery(authToken, keystoneUserId, CREATE_IMAGES_TABLE);
			
			log.debug("Executing query...\n"+CREATE_RESOURCE_TABLES_TABLE);
			//quickJob.setQry(CREATE_RESOURCE_TABLES_TABLE);
			//stub.executeQuickJob(quickJob);
			client.executeQuickQuery(authToken, keystoneUserId, CREATE_RESOURCE_TABLES_TABLE);
			
			log.debug("Executing query...\n"+CREATE_TABLE_METADATA_TABLE);
			//quickJob.setQry(CREATE_TABLE_METADATA_TABLE);
			//stub.executeQuickJob(quickJob);
			client.executeQuickQuery(authToken, keystoneUserId, CREATE_TABLE_METADATA_TABLE);
			
			log.debug("Executing query...\n"+CREATE_TABLE_SCHEMA_VIEW);
			//quickJob.setQry(CREATE_TABLE_SCHEMA_VIEW);
			//stub.executeQuickJob(quickJob);
			client.executeQuickQuery(authToken, keystoneUserId, CREATE_TABLE_SCHEMA_VIEW);
		}
		catch (Exception e) {
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
		String keystoneUserId = metadata.get("owner");
		
		String resourceName = metadata.get(TikaCoreProperties.TITLE);
		String resourcePath = metadata.get(TikaCoreProperties.SOURCE);
		String contentType = metadata.get(TikaCoreProperties.TYPE);
		String dateCreated = metadata.get("section0:DATE"); // FITS file creation date according to FITS specification

		String dateProcessed = metadata.get(TikaCoreProperties.METADATA_DATE);
		// This query returns the last created resourceId
		String query = 
			"INSERT INTO "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME)
			+" ([resourceName],[resourcePath],[contentType],[dateCreated],[dateProcessed]) VALUES ("
			+ databaseFormat.formatCharString(resourceName)+","
			+ databaseFormat.formatCharString(resourcePath)+","
			+ databaseFormat.formatCharString(contentType)+","
			+ databaseFormat.formatDateTime(dateCreated)+","
			+ databaseFormat.formatDateTime(dateProcessed)+"); SELECT SCOPE_IDENTITY();";
		log.debug("Executing query...\n"+query.toString());

		//quickJob.setQry(query);

		// Get query result
		try {
			//ExecuteQuickJobResponse res = stub.executeQuickJob(quickJob);
			//String responseString = res.getExecuteQuickJobResult();
			String responseString = client.executeQuickQuery(authToken, keystoneUserId, query);
			StringTokenizer st = new StringTokenizer(responseString,"\n");
			st.nextToken(); // Skip header
			int resourceId = Integer.parseInt(st.nextToken());
			return resourceId;
		}
		catch (Exception e){
			throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(RESOURCES_TABLE_NAME),e);
		}
	}
	
	private void updateKeyValue(int resourceId, Metadata metadata) throws DatabaseException {
		String keystoneUserId = metadata.get("owner");
		
		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO "+databaseFormat.formatObjectName(KEY_VALUE_TABLE_NAME)+" ([resourceId],[keyword],[value],[comment]) ");
		boolean first = true;
		for (String name: metadata.names()) {
			if (name.startsWith("section0:")) {
				String keyword = name.substring(name.indexOf(":")+1);
				String value = metadata.getValues(name)[0];
				String comment = " ";
				if (metadata.isMultiValued(name)) {
					comment = metadata.getValues(name)[1];
				}
				
				if (first) {
					query.append("\n  SELECT "); 
					first = false;
				}
				else {
					query.append(" UNION\n  SELECT ");
				}
				query.append(
					resourceId+","
					+databaseFormat.formatCharString(keyword)+","
					+databaseFormat.formatCharString(value)+","
					+databaseFormat.formatCharString(comment)
				);
			}
		}	
		if (!first) {
			log.debug("Executing query...\n"+query.toString());	
			//quickJob.setQry(query.toString());
			try {
				client.executeQuickQuery(authToken, keystoneUserId, query.toString());
			}
			catch (Exception e) {
				throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(KEY_VALUE_TABLE_NAME),e);
			}
		}
	}
	
	private void updateImages(int resourceId, Metadata metadata) throws DatabaseException {
		String keystoneUserId = metadata.get("owner");
		
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
		dateObs = (s==null || s.isEmpty())?null:databaseFormat.formatDateTime(s);
		
		s = metadata.get("section0:ORIGIN");
		origin = (s==null || s.isEmpty())?null:databaseFormat.formatCharString(s);
		
		s = metadata.get("section0:OBJECT");
		object = (s==null || s.isEmpty())?null:databaseFormat.formatCharString(s);
		
		s = metadata.get("section0:TELESCOP");
		telescop = (s==null || s.isEmpty())?null:databaseFormat.formatCharString(s);
		
		s = metadata.get("section0:EXPTIME");
		expTime = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:FILTER");
		filter = (s==null || s.isEmpty())?null:databaseFormat.formatCharString(s);
		
		s = metadata.get("section0:CRPIX1");
		crPix1 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CRPIX2");
		crPix2 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CRVAL1");
		crVal1 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CRVAL2");
		crVal2 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CTYPE1");
		cType1 = (s==null || s.isEmpty())?null:databaseFormat.formatCharString(s);
		
		s = metadata.get("section0:CTYPE2");
		cType2 = (s==null || s.isEmpty())?null:databaseFormat.formatCharString(s);
		
		s = metadata.get("section0:CD1_1");
		cd1_1 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CD1_2");
		cd1_2 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CD2_1");
		cd2_1 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		s = metadata.get("section0:CD2_2");
		cd2_2 = (s==null || s.isEmpty())?null:Double.parseDouble(s);
		
		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO "+databaseFormat.formatObjectName(IMAGES_TABLE_NAME)
			+" ([resourceId],[nAxis1],[nAxis2],[bitPix],[bZero],[bScale],[dateObs],[origin],[object],[telescop],[expTime],[filter],"
			+"[crPix1],[crPix2],[crVal1],[crVal2],[cType1],[cType2],[cd1_1],[cd1_2],[cd2_1],[cd2_2]) VALUES (");
		query.append(resourceId+",");
		query.append(nAxis1+",");
		query.append(nAxis2+",");
		query.append(bitPix+",");
		query.append(bZero+",");
		query.append(bScale+",");
		query.append(dateObs+",");
		query.append(origin+",");
		query.append(object+",");
		query.append(telescop+",");
		query.append(expTime+",");
		query.append(filter+",");
		query.append(crPix1+",");
		query.append(crPix2+",");
		query.append(crVal1+",");
		query.append(crVal2+",");
		query.append(cType1+",");
		query.append(cType2+",");
		query.append(cd1_1+",");
		query.append(cd1_2+",");
		query.append(cd2_1+",");
		query.append(cd2_2+")");
		
		log.debug("Executing query...\n"+query.toString());

		//quickJob.setQry(query.toString());
		try {
			client.executeQuickQuery(authToken, keystoneUserId, query.toString());
		}
		catch (Exception e) {
			throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(IMAGES_TABLE_NAME),e);
		}
	}
	
	private void updateResourceTables(int resourceId, Metadata metadata, ArrayList<AsciiTable> tables) throws DatabaseException {
		String keystoneUserId = metadata.get("owner");
		String resourceName = metadata.get(TikaCoreProperties.TITLE);
		for (AsciiTable table : tables) {
			int tableId = table.getTableId();
			
			//String tableName = "Table_"+resourceId+"_"+tableId;
			String tableName = "T_"+resourceName.replaceAll("\\W", "_")+"_"+resourceId+"_"+tableId;
			
			String updateResourceTablesQuery = 
				"INSERT INTO "+databaseFormat.formatObjectName(RESOURCE_TABLES_TABLE_NAME)
				+" ([resourceId],[tableId],[tableName]) VALUES("+resourceId+","+tableId+","+databaseFormat.formatCharString(tableName)+")";
			log.debug("Executing query...\n"+updateResourceTablesQuery);
			
			//quickJob.setQry(updateResourceTablesQuery);
			
			try {
				client.executeQuickQuery(authToken, keystoneUserId, updateResourceTablesQuery);
			}
			catch (Exception e) {
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
				//createTableQuery.append("\"");
				createTableQuery.append(databaseFormat.formatObjectName(table.getColumnNames()[i].replaceAll("\\[", "").replaceAll("\\]", "")));
				//createTableQuery.append("\"");
				createTableQuery.append(" ");
				createTableQuery.append(databaseFormat.getDatabaseType(table.getColumnTypes()[i]));
			}
			createTableQuery.append(")");
			log.debug("Executing query...\n"+createTableQuery.toString());
			
			//quickJob.setQry(createTableQuery.toString());
			
			try {
				client.executeQuickQuery(authToken, keystoneUserId, createTableQuery.toString());
			}
			catch (Exception e) {
				throw new DatabaseException("Table creation failed "+databaseFormat.formatObjectName(tableName),e);
			}			
			

			
			int startRow = 0;
			int BATCH_SIZE = 250;
			ArrayList<String[]> rows = table.getRows();
			
			while (startRow < rows.size()) {
				StringBuilder insertRowQuery = new StringBuilder();
				insertRowQuery.append("INSERT INTO "+databaseFormat.formatObjectName(tableName)+" ");
				int n;
				if (startRow + BATCH_SIZE > rows.size()) {
					n = rows.size() - startRow; 
				}
				else {
					n = BATCH_SIZE;
				}
				
				boolean first = true;
				for (int currentRow = startRow; currentRow<(startRow+n); currentRow++) {
					String[] row = rows.get(currentRow);
					StringBuilder rowValues = new StringBuilder();
					for (int i=0; i<table.getColumns(); i++) {
						if (i!=0) {
							rowValues.append(",");
						}
						String s = row[i];
						if (table.getColumnTypes()[i].charAt(0) == 'A') {
							s = databaseFormat.formatCharString(s);
						}
						else if (s.trim().isEmpty() || "nan".equals(s.trim().toLowerCase())) {
							s = "NULL";
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
					//quickJob.setQry(insertRowQuery.toString());
					try {
						client.executeQuickQuery(authToken, keystoneUserId, insertRowQuery.toString());
					}
					catch (Exception e) {
						throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(tableName),e);
					}
				}
				
				startRow += BATCH_SIZE;
			}
			

			String prefix = "section"+tableId+":";
			
			StringBuilder insertTableMetadataQuery = new StringBuilder();
			insertTableMetadataQuery.append("INSERT INTO "+databaseFormat.formatObjectName(TABLE_METADATA_TABLE_NAME)+" ([resourceId],[tableId],[keyword],[value],[comment]) ");
			boolean first = true;
			for (String name: metadata.names()) {
				if (name.startsWith(prefix)) {
					String keyword = name.substring(name.indexOf(":")+1);
					
					String value = metadata.getValues(name)[0];
					String comment = metadata.getValues(name)[1];
					
					if (first) {
						insertTableMetadataQuery.append("\n  SELECT "); 
						first = false;
					}
					else {
						insertTableMetadataQuery.append(" UNION\n  SELECT ");
					}
					insertTableMetadataQuery.append(
						resourceId+","
						+tableId+","
						+databaseFormat.formatCharString(keyword)+","
						+databaseFormat.formatCharString(value)+","
						+databaseFormat.formatCharString(comment)
					);
				}
			}	
			if (!first) {
				log.debug("Executing query...\n"+insertTableMetadataQuery.toString());
				
				//quickJob.setQry(insertTableMetadataQuery.toString());
				try {
					client.executeQuickQuery(authToken, keystoneUserId, insertTableMetadataQuery.toString());
				}
				catch (Exception e) {
					throw new DatabaseException("Table update failed "+databaseFormat.formatObjectName(tableName),e);
				}
			}
			metadata.add("EXTERNAL_LINKS", endpoint+"/MyDB.aspx?ObjName="+tableName+"&ObjType=TABLE&context=MyDB&type=normal");
		}
	}
	
}
