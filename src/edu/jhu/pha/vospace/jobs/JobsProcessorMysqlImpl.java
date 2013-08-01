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
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.protocol.ProtocolHandler;
import edu.jhu.pha.vospace.rest.JobDescription;

/**
 * The JobsProcessor class creates a separate thread, waiting for new jobs to appear in the queue.
 * If a new client-initialized job appears, the process sets the needed fields of the job and sets the finished status.
 * @author Dmitry Mishin
 */
@Deprecated
public class JobsProcessorMysqlImpl extends JobsProcessor {

	private static final long serialVersionUID = -7347829485565922282L;
	private static final Logger logger = Logger.getLogger(JobsProcessorMysqlImpl.class);
    private ExecutorService service;
	private Thread jobsThread = null;
	private static Vector<Future> workers;
    
	public JobsProcessorMysqlImpl() {
        service = Executors.newCachedThreadPool();
        workers = new Vector<Future>();
	}
	
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.jobs.JobsProcessor#destroy()
	 */
	@Override
	public void destroy() {
		logger.debug("INTERRUPTING!");
		jobsThread.interrupt();
	}
	
	/* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.JobsProcessor#getProtocolHandler(java.lang.String, edu.jhu.pha.vospace.TransferThread)
	 */
	@Override
	public ProtocolHandler getProtocolHandler(String protocolUri, TransferThread thread) {
		if(null != protocolHandlers.get(protocolUri)){
			try {
				ProtocolHandler handlerInst = (ProtocolHandler)protocolHandlers.get(protocolUri).getConstructor().newInstance();
				return handlerInst;
			} catch(InvocationTargetException e) {
				logger.error(e);
			} catch (IllegalArgumentException e) {
				logger.error(e);
			} catch (SecurityException e) {
				logger.error(e);
			} catch (InstantiationException e) {
				logger.error(e);
			} catch (IllegalAccessException e) {
				logger.error(e);
			} catch (NoSuchMethodException e) {
				logger.error(e);
			}
		}
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			// The main cycle to process the jobs
			while (!jobsThread.isInterrupted()) {
				
				for(Iterator<Future> it = workers.iterator(); it.hasNext();){
					Future next = it.next();
					if(next.isDone()){
						it.remove();
						logger.debug("Job "+next+" is removed from the workers.");
					}
				}
				
				if(workers.size() >= jobsPoolSize) {
					logger.debug("Waiting for a jobs pool, size: "+workers.size());
					synchronized(JobsProcessor.class) {
						JobsProcessor.class.wait();
					}
					logger.debug("End waiting for a jobs pool, size: "+workers.size());
				} else {
			    	//logger.debug("Looking for a job");

					final JobsProcessor currentProc = this;
					
			        DbPoolServlet.goSql("Retreiving new job",
			        		"select id, json_notation from jobs where state = ? and (DIRECTION != 'PULLFROMVOSPACE' and DIRECTION != 'PUSHTOVOSPACE') order by starttime limit 1",
			                new SqlWorker<Boolean>() {
			                    @Override
			                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
			    			    	JobDescription job = null;
			                        stmt.setString(1, JobDescription.STATE.PENDING.toString());
									ResultSet resSet = stmt.executeQuery();
									if(resSet.next()){
									    if(conn.createStatement().executeUpdate("update jobs set state = '"+JobDescription.STATE.RUN.toString()+"' where id = '"+resSet.getString("id")+"' and state = '"+JobDescription.STATE.PENDING.toString()+"';") > 0) {
									    	logger.debug("Updated the job to Running, running the job.");
											byte[] jobBytes = resSet.getBytes("json_notation");
											try {
												job = (new ObjectMapper()).readValue(jobBytes, 0, jobBytes.length, JobDescription.class);
											} catch(IOException ex) {
												logger.error("Error mapping the job to Json");
											}
									    } else {
									    	logger.debug("Update returned 0");
									    }
									}

									if(null != job){
								    	logger.debug("There's a submited job! "+job.getId());
								    	
						    			TransferThread thread = new TransferThread(job, currentProc);
						    			thread.setName(job.getId());
						    			
						    			Future future = service.submit(thread);
						    			
						    			workers.add(future);
							    	} else {
										try {
											Thread.sleep(1000*10);
										} catch (InterruptedException e) {
										}
									}

									return true;
			                    }
			                }
			        );
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			logger.error(e);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				service.awaitTermination(3, TimeUnit.SECONDS);
				logger.debug("Exiting the jobs process.");
			} catch (InterruptedException e) {
				logger.error(e);
			}
		}
		
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.jobs.JobsProcessor#start()
	 */
	@Override
	public void start() {
		jobsThread = new Thread(this);
		jobsThread.setDaemon(true);
		jobsThread.start();
	}

}
