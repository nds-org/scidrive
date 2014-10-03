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

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.mail.internet.MimeUtility;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.api.exceptions.PermissionDeniedException;
import edu.jhu.pha.vospace.jobs.JobsProcessor;
import edu.jhu.pha.vospace.meta.MetaStore;
import edu.jhu.pha.vospace.meta.MetaStoreFactory;
import edu.jhu.pha.vospace.node.DataNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.rest.JobDescription.STATE;

/**
 * Provides the REST service for /data/ path: the functions for manipulating the nodes data content
 * @author Dmitry Mishin
 */
@Path("/jldata/")
public class JLDataController {
	
	private static final Logger logger = Logger.getLogger(DataController.class);
	private @Context SecurityContext security; 
	
	/**
	 * Returns the data of a transfer
	 * @param jobId Job identifier
	 * @return transfer representation
	 */
	@GET @Path("{jobid}")
	public Response getTransferData(@HeaderParam("user-agent") String userAgent, @PathParam("jobid") String jobId) {
		JobDescription job = JobsProcessor.getJob(UUID.fromString(jobId));
		if(null == job)
			throw new NotFoundException("The job "+jobId+" is not found.");

		MetaStore store = MetaStoreFactory.getMetaStore(job.getUsername());
		
		VospaceId targetId=job.getTargetId();
		Node node = NodeFactory.getNode(targetId, job.getUsername());
		
		if(job.getDirection().equals(JobDescription.DIRECTION.PULLFROMVOSPACE)){
			
			JobsProcessor.modifyJobState(job, STATE.RUN);
			
			logger.debug("Downloading node "+targetId.toString());
			
			Response.ResponseBuilder resp = Response.ok();
			
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				ByteArrayOutputStream context = new ByteArrayOutputStream();
                                String ls = System.getProperty("line.separator");

                                out.write("{".getBytes(StandardCharsets.UTF_8));
                                context.write((ls + "\"@context\": {").getBytes(StandardCharsets.UTF_8));
                                int i = 0;
                                for (Map.Entry<String, String> entry : store.getProperties(targetId).entrySet())
                                {
                                    i++;
                                    if (i != 1) {
                                        out.write(("," + ls).getBytes(StandardCharsets.UTF_8));
                                        context.write(("," + ls).getBytes(StandardCharsets.UTF_8));
                                    } else {
                                        out.write(ls.getBytes(StandardCharsets.UTF_8));
                                        context.write(ls.getBytes(StandardCharsets.UTF_8));
                                    }
                                    String[] sp = entry.getKey().split("#");
                                    context.write(("\"" + sp[1] + "\" : \"" + entry.getKey() + "\"").getBytes(StandardCharsets.UTF_8));
                                    out.write(("\"" + sp[1] + "\": ").getBytes(StandardCharsets.UTF_8));
                                    out.write(("\"" + entry.getValue() + "\"").getBytes(StandardCharsets.UTF_8));
                                }
                                context.write((ls + "}").getBytes(StandardCharsets.UTF_8));
                                if (i != 0) {
                                        out.write((",").getBytes(StandardCharsets.UTF_8));
                                }
                                context.writeTo(out);
                                out.write((ls + "}").getBytes(StandardCharsets.UTF_8));
                                InputStream dataInp = new ByteArrayInputStream(out.toByteArray());
				
				String fileName;
				boolean isInternetExplorer = (userAgent.indexOf("MSIE") > -1);
				if (isInternetExplorer) {
				    fileName = URLEncoder.encode(targetId.getNodePath().getNodeName(), "utf-8");
				} else {
					fileName = MimeUtility.encodeWord(targetId.getNodePath().getNodeName());
				}
				
				if(!node.getType().equals(NodeType.CONTAINER_NODE)) {
					resp.header("Content-Disposition", "attachment; filename=\""+fileName+"\"");
					resp.header("Content-Length", Long.toString(out.size()));
					resp.header("Content-Type", node.getNodeInfo().getContentType());
				} else {
					resp.header("Content-Disposition", "attachment; filename=\""+fileName+".tar\"");
					resp.header("Content-Type", "application/tar");
				}
				JobsProcessor.modifyJobState(job, STATE.COMPLETED);
				resp.entity(dataInp);
				return resp.build();
			} catch(InternalServerErrorException ex) {
				JobsProcessor.modifyJobState(job, STATE.ERROR);
				throw ex;
			} catch(NotFoundException ex) {
				JobsProcessor.modifyJobState(job, STATE.ERROR);
				throw ex;
			} catch(PermissionDeniedException ex) {
				JobsProcessor.modifyJobState(job, STATE.ERROR);
				throw ex;
			} catch(IOException ex) {
				JobsProcessor.modifyJobState(job, STATE.ERROR);
				throw new InternalServerErrorException(ex);
			//} catch (UnsupportedEncodingException ex) {
			//	JobsProcessor.modifyJobState(job, STATE.ERROR);
			//	throw new InternalServerErrorException(ex);
			}
		}
		
		throw new InternalServerErrorException("The job "+job.getDirection()+" is unsupported in this path.");
	}
}
