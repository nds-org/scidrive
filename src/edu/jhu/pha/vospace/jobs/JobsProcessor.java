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
package edu.jhu.pha.vospace.jobs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.protocol.ProtocolHandler;
import edu.jhu.pha.vospace.rest.JobDescription;
import edu.jhu.pha.vospace.rest.JobDescription.STATE;

public abstract class JobsProcessor implements Runnable  {

	private static final Logger logger = Logger.getLogger(JobsProcessor.class);
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	static Configuration conf = SettingsServlet.getConfig();
	Hashtable<String, Class<? extends ProtocolHandler>> protocolHandlers;
	int jobsPoolSize;

	public JobsProcessor() {
        jobsPoolSize = conf.getInt("maxtransfers");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        
		initProtocolHandlers();
	}
		
	public static Class<? extends JobsProcessor> getImplClass() {
		Class<? extends JobsProcessor> jobsClass;
		try {
			jobsClass = (Class<? extends JobsProcessor>) Class.forName(conf.getString("jobsprocessor.class"));
			return jobsClass;
		} catch (ClassNotFoundException e) {
			logger.error("Error in configuration: can't find the jobs processor class");
			return null;
		}
	}
	
	/**
	 * Returns the JobDescription object serialized from the database record
	 * @param jobId The identifier of a job
	 * @return The job java object
	 */
	public static JobDescription getJob(final UUID jobId) {
        return DbPoolServlet.goSql("GetJob request",
        		"select json_notation, note from jobs where id = ?",
                new SqlWorker<JobDescription>() {
                    @Override
                    public JobDescription go(Connection conn, PreparedStatement stmt) throws SQLException {
                		JobDescription returnJob = null;
                        stmt.setString(1, jobId.toString());
                        ResultSet rs = stmt.executeQuery();
            			if(rs.next()) {
            				byte[] jobJsonNotation = rs.getBytes(1);
            				try {
	            				returnJob = (new ObjectMapper()).readValue(jobJsonNotation, 0, jobJsonNotation.length, JobDescription.class);
	            				returnJob.setNote(rs.getString("note"));
            				} catch(JsonMappingException ex) { // Shouldn't happen
            					throw new InternalServerErrorException(ex.getMessage());
            				} catch (JsonParseException ex) {
            					throw new InternalServerErrorException(ex.getMessage());
							} catch (IOException ex) {
            					throw new InternalServerErrorException(ex.getMessage());
							}
            			}
            			return returnJob;
                    }
                }
        );
	}
	
	public static void modifyJobState(JobDescription job, STATE state) {
		modifyJobState(job, state, null);
	}

	public static void modifyJobState(final JobDescription job, final STATE state, final String note) {

		if(null == job.getEndTime() && (state == STATE.COMPLETED || state == STATE.ERROR)){
			job.setEndTime(new Date());
		}

		job.setState(state);
		final byte[] jobBytes;
		try {
			jobBytes = (new ObjectMapper()).writeValueAsBytes(job);
		} catch(Exception ex) {
			throw new InternalServerErrorException(ex.getMessage());
		}
		
        DbPoolServlet.goSql("Modify job",
        		"update jobs set endtime = ?, state = ?, json_notation = ?, note = ? where id = ?",
                new SqlWorker<Integer>() {
                    @Override
                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
            			stmt.setString(1, (null == job.getEndTime())?null:dateFormat.format(job.getEndTime().getTime()));
            			stmt.setString(2, job.getState().toString());
            			stmt.setBytes(3, jobBytes);
            			stmt.setString(4, (null == note)?"":note);
            			stmt.setString(5, job.getId());
            			return stmt.executeUpdate();
                    }
                }
        );
		logger.debug("Job "+job.getId()+" is modified. "+job.getState());
	}
	
	/**
	 * Adds the new job to the SQL database
	 * @param job The job description object
	 */
	protected static void submitJob(final String login, final JobDescription job) {
		final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		final byte[] jobSer;
		try {
			jobSer = (new ObjectMapper()).writeValueAsBytes(job);
		} catch(Exception ex) {
			ex.printStackTrace();
			throw new InternalServerErrorException(ex.getMessage());
		}
		
        DbPoolServlet.goSql("Submit job",
        		"insert into jobs (id,user_id,starttime,state,direction,target,json_notation) select ?, user_id, ?,?,?,?,? from user_identities WHERE identity = ?",
                new SqlWorker<Integer>() {
                    @Override
                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
            			stmt.setString(1, job.getId());
            			stmt.setString(2, dateFormat.format(job.getStartTime().getTime()));
            			stmt.setString(3, job.getState().toString());
            			stmt.setString(4, job.getDirection().toString());
            			stmt.setString(5, job.getTarget().toString());
            			stmt.setBytes(6, jobSer);
            			stmt.setString(7, login);
            			return stmt.executeUpdate();
                    }
                }
        );
	}

	public abstract void destroy();

	public abstract ProtocolHandler getProtocolHandler(String protocolUri, TransferThread thread);

	
	private void initProtocolHandlers(){
		String confProtocolsPrefix = "transfers.protocol.handler";
		protocolHandlers = new Hashtable<String, Class<? extends ProtocolHandler>>();
		for(Iterator<String> it = conf.getKeys(confProtocolsPrefix); it.hasNext();){
			String protocolHandlerKey = it.next();
			String protocolName = protocolHandlerKey.substring(confProtocolsPrefix.length()+1);
			try {
				Class<? extends ProtocolHandler> handlerClass = (Class<? extends ProtocolHandler>)Class.forName(conf.getString(protocolHandlerKey));
				protocolHandlers.put(conf.getString("transfers.protocol."+protocolName), handlerClass);
			} catch(ClassNotFoundException ex) {
				logger.error("Unable to initialise the protocol handler "+protocolName+": Class "+conf.getString(protocolHandlerKey)+" not found.");
			}
		}
	}

	/**
	 * Start the jobs queue processing
	 */
	public abstract void start();

}
