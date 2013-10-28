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
package edu.jhu.pha.vospace.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.jobs.JobException;
import edu.jhu.pha.vospace.jobs.MyHttpConnectionPoolProvider;
import edu.jhu.pha.vospace.node.DataNode;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.rest.JobDescription;

/**
 * This class handles the implementation details for the HTTP 1.1 GET protocol
 */
public class HttpGetProtocolHandler implements ProtocolHandler {

	private static final Logger logger = Logger.getLogger(HttpGetProtocolHandler.class);
	private static final Pattern fileNamePattern = Pattern.compile("filename=\"(.*?)\"");
	
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.protocol.ProtocolHandler#getUri()
	 */
	@Override
	public String getUri() {
    	return SettingsServlet.getConfig().getString("transfers.protocol.httpget");
    }

    /*
     * (non-Javadoc)
     * @see edu.jhu.pha.vospace.protocol.ProtocolHandler#invoke(edu.jhu.pha.vospace.rest.JobDescription)
     */
	@Override
    public void invoke(JobDescription job) throws IOException, JobException, URISyntaxException{
		String getFileUrl = job.getProtocols().get(SettingsServlet.getConfig().getString("transfers.protocol.httpget"));
		
		HttpClient client = MyHttpConnectionPoolProvider.getHttpClient();
		
		HttpGet get = new HttpGet(getFileUrl);

		InputStream fileInp = null;
		
		try {
			HttpResponse response = client.execute(get);
			
			if(response.getStatusLine().getStatusCode() == 200) {
				fileInp = response.getEntity().getContent();

				logger.debug("Auto node: "+job.getTargetId().toString().endsWith("/.auto"));

				//TODO make also for container
				String fileName = FilenameUtils.getName(getFileUrl);

				Header dispositionHeader = response.getFirstHeader("content-disposition");
				if(null != dispositionHeader && dispositionHeader.getValue().indexOf("filename") > 0){
					Matcher matcher = fileNamePattern.matcher(dispositionHeader.getValue());
					if (matcher.find())	{
					    fileName = matcher.group(1);
					}
				}
				
				if(job.getTargetId().toString().endsWith("/.auto") && null != fileName){
					logger.debug("Auto node success "+fileName);
					VospaceId newId = new VospaceId(job.getTargetId().toString().replaceFirst(".auto", fileName));
					DataNode targetNode = (DataNode)NodeFactory.createNode(newId, job.getUsername(), NodeType.DATA_NODE);
					targetNode.setNode(null);
					targetNode.setData(fileInp);
				} else {
					DataNode targetNode = (DataNode)NodeFactory.getNode(job.getTargetId(), job.getUsername());
					targetNode.setData(fileInp);
				}
			} else {
				logger.error("Error processing job "+job.getId()+": "+response.getStatusLine().getStatusCode()+" "+response.getStatusLine().getReasonPhrase());
				throw new JobException("Error processing job "+job.getId()+": "+response.getStatusLine().getStatusCode()+" "+response.getStatusLine().getReasonPhrase());
			}
		} catch(IOException ex) {
			ex.printStackTrace();
			get.abort();
			throw ex;
		} finally {
			try {
				if(null != fileInp) fileInp.close();
			} catch (IOException e) {}
		}
    }

}
