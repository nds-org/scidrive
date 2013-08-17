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
package edu.jhu.pha.vospace.rest;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

import com.generationjava.io.xml.SimpleXmlWriter;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.BadRequestException;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.jobs.JobsProcessor;
import edu.jhu.pha.vospace.oauth.VoboxUser;
import edu.jhu.pha.vospace.rest.JobDescription.DIRECTION;
import edu.jhu.pha.vospace.rest.JobDescription.STATE;

/**
 * Provides the REST service for /transfers/ path: the functions for manipulating the transfer jobs
 * @author Dmitry Mishin
 */
@Path("/transfers/")
public class TransfersController {

	private static final Logger logger = Logger.getLogger(TransfersController.class);
	private static Configuration conf = SettingsServlet.getConfig();;
	private @Context SecurityContext security; 
	
	private static final String UWS_NAMESPACE = "http://www.ivoa.net/xml/UWS/v1.0";
	private static final String VOS_NAMESPACE = "http://www.ivoa.net/xml/VOSpace/v2.0";
	private static final String XLINK_NAMESPACE = "http://www.w3.org/1999/xlink";
	private static final String XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance";
	private static final String SCHEMA_LOCATION = "http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd";

	/**
	 * Manages the transfers methods:
	 * sending data to a service (pushToVoSpace)
	 * importing data into a service (pullToVoSpace)
	 * reading data from a service (pullFromVoSpace)
	 * sending data from a service (pushFromVoSpace)
	 * @param fullPath the path of the node
	 * @param fileDataInp the XML representation of the job
	 * @return HTTP result
	 */
	@POST
	@Produces(MediaType.TEXT_XML)
	@RolesAllowed({"user"})
    public Response transferNodePost(String xmlNode) {
		logger.debug("Got new job");

		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		
		UUID jobUID = submitJob(xmlNode, user.getName());

		try {
			//String redirectUri = conf.getString("application.url")+"/transfers/"+jobUID.toString();
			//ResponseBuilder respBuilder = Response.seeOther(new URI(redirectUri));
			//respBuilder.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
			//respBuilder.header("Access-Control-Allow-Origin", "http://dimm.pha.jhu.edu");
			//respBuilder.header("Location", redirectUri);
			//respBuilder.entity(redirectUri);
			//return respBuilder.build();
			return Response.ok(getJob(jobUID.toString())).build();
		} catch (Exception e) {
			throw new InternalServerErrorException("InternalFault");
		}
	}

	/**
	 * Submit the job to database
	 * @param xmlNode the job XML document
	 * @param username the username of the job owner
	 * @return the job ID
	 */
	public static UUID submitJob(String xmlNode, String username) {
		StringReader strRead = new StringReader(xmlNode);
		UUID jobUID = UUID.randomUUID();
		try {
			
			JobDescription job = new JobDescription();
			job.setId(jobUID.toString());
			job.setUsername(username);
			job.setStartTime(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime());
			job.setState(JobDescription.STATE.PENDING);

			SAXBuilder xmlBuilder = new SAXBuilder();
			Element nodeElm = xmlBuilder.build(strRead).getRootElement();
			List<Element> paramNodes = nodeElm.getChildren();
			for(Iterator<Element> it = paramNodes.iterator(); it.hasNext();){
				Element param = it.next();
				if(param.getName().equals("target")){
					try {
						job.setTarget(param.getValue());
					} catch (URISyntaxException e) {
						logger.error("Error in job parse: "+e.getMessage());
						throw new BadRequestException("InvalidURI");
					}
				} else if(param.getName().equals("direction")){
					
					JobDescription.DIRECTION direct = JobDescription.DIRECTION.LOCAL; 
					if(param.getValue().toUpperCase().endsWith("SPACE"))
						direct = JobDescription.DIRECTION.valueOf(param.getValue().toUpperCase());

					job.setDirection(direct);
					
					if(direct == JobDescription.DIRECTION.PULLFROMVOSPACE) {
						job.addProtocol(conf.getString("transfers.protocol.httpget"), conf.getString("application.url")+"/data/"+job.getId());
					} else if(direct == JobDescription.DIRECTION.PUSHTOVOSPACE) {
						job.addProtocol(conf.getString("transfers.protocol.httpput"), conf.getString("application.url")+"/data/"+job.getId());
					} else if(direct == JobDescription.DIRECTION.LOCAL)  {
						try {
							job.setDirectionTarget(param.getValue());
						} catch (URISyntaxException e) {
							logger.error("Error in job parse: "+e.getMessage());
							throw new BadRequestException("InvalidURI");
						}
					}
				} else if(param.getName().equals("view")){
					job.addView(param.getValue());
				} else if(param.getName().equals("keepBytes")){
					job.setKeepBytes(Boolean.parseBoolean(param.getValue()));
				} else if(param.getName().equals("protocol")){
					String protocol = param.getAttributeValue("uri");
					String protocolEndpoint = param.getChildText("protocolEndpoint", Namespace.getNamespace(VOS_NAMESPACE));
					
					if(job.getDirection().equals(DIRECTION.PULLFROMVOSPACE) || job.getDirection().equals(DIRECTION.PUSHTOVOSPACE)){
						protocolEndpoint = conf.getString("application.url")+"/data/"+job.getId();
					}
					
					if(null != protocol && null != protocolEndpoint)
						job.addProtocol(protocol, protocolEndpoint);
					else
						throw new BadRequestException("InvalidArgument");
				}
			}
			
			JobsProcessor.getDefaultImpl().submitJob(username, job);
		} catch (JDOMException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		} catch (IOException e) {
			logger.error(e);
			throw new InternalServerErrorException(e);
		} catch (IllegalArgumentException e) {
			logger.error("Error calling the job task: "+e.getMessage());
			throw new InternalServerErrorException("InternalFault");
		} finally {
			strRead.close();
		}
		return jobUID;
	}
	
	/**
	 * Returns the transfer representation
	 * @param jobId Job identifier
	 * @return transfer representation
	 */
	@GET @Path("{jobId}")
	@Produces(MediaType.TEXT_XML)
	@RolesAllowed({"user"})
	public String getTransferDetails(@PathParam("jobId") String jobId) {
		return getJob(jobId);
	}

	
	private String getJob(String jobId) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		StringWriter writ = new StringWriter();
		SimpleXmlWriter xw = new SimpleXmlWriter(writ);
		try {
			JobDescription job = JobsProcessor.getJob(UUID.fromString(jobId));
			
			xw.writeEntity("uws:job").
				writeAttribute("xmlns:uws", UWS_NAMESPACE).
				writeAttribute("xmlns:vos", VOS_NAMESPACE).
				writeAttribute("xmlns:xlink", XLINK_NAMESPACE).
				writeAttribute("xmlns:xsi", XSI_NAMESPACE).
				writeAttribute("xmlns:schemaLocation", SCHEMA_LOCATION);

		  
			xw.writeEntity("uws:jobId").writeText(jobId).endEntity();
			
			xw.writeEntity("uws:ownerId").writeAttribute("xsi:nil", "true").endEntity();

			if(null == job) {
				xw.writeEntity("uws:phase").writeText(STATE.ERROR).endEntity();
				xw.writeEntity("uws:errorSummary").writeText("Internal Fault").endEntity();
				xw.writeEntity("result").writeAttribute("id", "transferDetails").writeAttribute("xlink:href",jobId+"/error").endEntity();
			} else {
					xw.writeEntity("uws:phase").writeText(job.getState().toString()).endEntity();
				
				xw.writeEntity("uws:startTime");
				if(null != job.getStartTime())
					xw.writeText(dateFormat.format(job.getStartTime()));
				else
					xw.writeAttribute("xsi:nil", "true");
				xw.endEntity();
	
				xw.writeEntity("uws:endTime");
				if(null != job.getEndTime())
					xw.writeText(dateFormat.format(job.getEndTime()));
				else
					xw.writeAttribute("xsi:nil", "true");
				xw.endEntity();
				
				xw.writeEntity("uws:executionDuration").writeText("0").endEntity();
				xw.writeEntity("uws:destruction").writeAttribute("xsi:nil", "true").endEntity();
				
				xw.writeEntity("uws:jobInfo");

					xw.writeEntity("vos:transfer");

						switch(job.getDirection()){
							case PULLFROMVOSPACE:
								xw.writeEntity("vos:direction").writeText("pullFromVoSpace").endEntity();
								break;
							case PULLTOVOSPACE:
								xw.writeEntity("vos:direction").writeText("pullToVoSpace").endEntity();
								break;
							case PUSHFROMVOSPACE:
								xw.writeEntity("vos:direction").writeText("pushFromVoSpace").endEntity();
								break;
							case PUSHTOVOSPACE:
								xw.writeEntity("vos:direction").writeText("pushToVoSpace").endEntity();
								break;
							case LOCAL:
								xw.writeEntity("vos:direction").writeText(job.getDirectionTarget()).endEntity();
								break;
						}

						for(Iterator<String> it = job.getProtocols().keySet().iterator(); it.hasNext();){
							String protocol = it.next();
							String protocolEndpoint = job.getProtocols().get(protocol);
							xw.writeEntity("vos:protocol").writeAttribute("uri", protocol);
							if(null != protocolEndpoint && !protocolEndpoint.isEmpty()){
								xw.writeEntity("vos:protocolEndpoint").writeText(protocolEndpoint).endEntity();
							}
							xw.endEntity();
						}
						xw.writeEntity("vos:target").writeText(job.getTarget()).endEntity();
						for(String view: job.getViews())
							xw.writeEntity("vos:view").writeText(view).endEntity();

					xw.endEntity();

				xw.endEntity();
		
				xw.writeEntity("uws:results");
				xw.writeEntity("result").writeAttribute("id", "transferDetails").writeAttribute("xlink:href",conf.getString("application.url")+"/transfers/"+job.getId()+"/results/details").endEntity();
				xw.endEntity();
			}
	
			
			xw.endEntity();
			xw.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		} catch(IllegalArgumentException ex) {
			throw new BadRequestException(ex);
		}
		return writ.toString();
	}
	
	@POST @Path("{jobid}/phase")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@RolesAllowed({"user"})
    public Response changeJobPhase(@PathParam("jobid") String jobId, String phase) {
		logger.debug("Got new phase "+phase+" for job "+jobId);
		
		/**TODO make something here */
		return Response.ok().build();
	}
	
	/**
	 * Returns the transfer error representation
	 * @param jobId Job identifier
	 * @return transfer representation
	 */
	@GET @Path("{jobid}/error")
	@Produces(MediaType.TEXT_PLAIN)
	@RolesAllowed({"user"})
	public String getTransferErrorDetails(@PathParam("jobid") String jobId) {
		JobDescription job = JobsProcessor.getJob(UUID.fromString(jobId));

		StringBuffer errorDescr = new StringBuffer();
		if(null == job)
			errorDescr.append("The job "+jobId+" is not found.\n");
		else
			errorDescr.append(job.getNote());
		return errorDescr.toString();
	}

	/**
	 * Returns the transfer result
	 * @param jobId Job identifier
	 * @return transfer result
	 */
	@GET @Path("{jobid}/results")
	@Produces(MediaType.TEXT_XML)
	@RolesAllowed({"user"})
	public String getTransferResults(@PathParam("jobid") String jobId) {
		JobDescription job = JobsProcessor.getJob(UUID.fromString(jobId));

		if(null == job)
			throw new NotFoundException("The job "+jobId+" is not found.");

		//if(!job.getState().equals(JobDescription.STATE.COMPLETED))
		//	return "";
		
		StringWriter writ = new StringWriter();
		SimpleXmlWriter xw = new SimpleXmlWriter(writ);
		try {
			xw.writeEntity("ns0:results").writeAttribute("xmlns:ns0", UWS_NAMESPACE);
				xw.writeEntity("ns0:result").
					writeAttribute("xmlns:ns1", XLINK_NAMESPACE).
					writeAttribute("id","transferDetails").
					writeAttribute("ns1:href",conf.getString("application.url")+"/transfers/"+jobId+"/results/details").endEntity();
			xw.endEntity();
			xw.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
		return writ.toString();
	}

	/**
	 * Returns the transfer results details
	 * @param jobId Job identifier
	 * @return transfer results details
	 */
	@GET @Path("{jobid}/results/details")
	@Produces(MediaType.TEXT_XML)
	@RolesAllowed({"user"})
	public String getTransferResultsDetails(@PathParam("jobid") String jobId) {
		JobDescription job = JobsProcessor.getJob(UUID.fromString(jobId));
		
		if(null == job)
			throw new NotFoundException("The job "+jobId+" is not found.");

		StringWriter writ = new StringWriter();
		SimpleXmlWriter xw = new SimpleXmlWriter(writ);
		try {
			xw.writeEntity("transfer");

			xw.writeEntity("target").writeText(job.getTarget()).endEntity();
			switch(job.getDirection()){
				case PULLFROMVOSPACE:
					xw.writeEntity("vos:direction").writeText("pullFromVoSpace").endEntity();
					break;
				case PULLTOVOSPACE:
					xw.writeEntity("vos:direction").writeText("pullToVoSpace").endEntity();
					break;
				case PUSHFROMVOSPACE:
					xw.writeEntity("vos:direction").writeText("pushFromVoSpace").endEntity();
					break;
				case PUSHTOVOSPACE:
					xw.writeEntity("vos:direction").writeText("pushToVoSpace").endEntity();
					break;
				case LOCAL:
					xw.writeEntity("vos:direction").writeText(job.getDirectionTarget()).endEntity();
					break;
			}
			for(String view: job.getViews())
				xw.writeEntity("view").writeText(view).endEntity();

			for(Iterator<String> it = job.getProtocols().keySet().iterator(); it.hasNext();){
				String protocol = it.next();
				String protocolEndpoint = job.getProtocols().get(protocol);
				xw.writeEntity("protocol").writeAttribute("uri", protocol);
				if(null != protocolEndpoint && !protocolEndpoint.isEmpty()){
					xw.writeEntity("endpoint").writeText(protocolEndpoint).endEntity();
				}
				xw.endEntity();
			}

			xw.endEntity();
			xw.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
		return writ.toString();
	}

	/**
	 * Returns the transfer error representation
	 * @param jobId Job identifier
	 * @return transfer representation
	 */
	@GET @Path("{jobid}/phase")
	@Produces(MediaType.TEXT_PLAIN)
	@RolesAllowed({"user"})
	public String getTransferPhase(@PathParam("jobid") String jobId) {
		JobDescription job = JobsProcessor.getJob(UUID.fromString(jobId));
		
		if(null == job){
			logger.error("Job "+jobId+" isn't found.");
			throw new NotFoundException("The job is not found.");
		}
		return job.getState().toString();
	}

	/**
	 * Returns the transfers queue
	 * @return transfer representation
	 */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@RolesAllowed({"user"})
	public String getTransfersQueue() {
		final VoboxUser user = ((VoboxUser)security.getUserPrincipal());
	    return DbPoolServlet.goSql("Get transfers queue",
	    		"select id, state, direction, starttime, endtime, target from jobs where login = ?",
	            new SqlWorker<String>() {
	                @Override
	                public String go(Connection conn, PreparedStatement stmt) throws SQLException {
	            		StringBuffer resultBuf = new StringBuffer();
	            		resultBuf.append("id, state, direction, starttime, endtime, path\n");

	            		stmt.setString(1, user.getName());

	            		ResultSet resSet = stmt.executeQuery();
	        			while(resSet.next()) {
	        				resultBuf.append(resSet.getString(1)+", ");
	        				resultBuf.append(resSet.getString(2)+", ");
	        				resultBuf.append(resSet.getString(3)+", ");
	        				resultBuf.append((null != resSet.getTimestamp(4)?resSet.getTimestamp(4):"")+", ");
	        				resultBuf.append((null != resSet.getTimestamp(5)?resSet.getTimestamp(5):"")+", ");
	        				resultBuf.append(resSet.getString(6));
	        				resultBuf.append("\n");
	        	    	}
	        			return resultBuf.toString();
	                }
	            }
	    );
	}
	
	
}
