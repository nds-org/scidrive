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

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.jobs.MyHttpConnectionPoolProvider;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.rest.JobDescription;
import edu.jhu.pha.vospace.storage.StorageManager;
import edu.jhu.pha.vospace.storage.StorageManagerFactory;

/**
 * This class handles the implementation details for the HTTP 1.1 PUT protocol
 */
public class HttpPutProtocolHandler implements ProtocolHandler {

	private static final Logger logger = Logger.getLogger(HttpPutProtocolHandler.class);
	
	/*
	 * (non-Javadoc)
	 * @see edu.caltech.vao.vospace.protocol.ProtocolHandler#getUri()
	 */
	@Override
	public String getUri() {
    	return SettingsServlet.getConfig().getString("transfers.protocol.httpput");
	}

	/*
	 * (non-Javadoc)
	 * @see edu.caltech.vao.vospace.protocol.ProtocolHandler#invoke(edu.jhu.pha.vospace.rest.JobDescription)
	 */
	@Override
    public void invoke(JobDescription job) throws IOException {
		String putFileUrl = job.getProtocols().get(SettingsServlet.getConfig().getString("transfers.protocol.httpput"));
		
		StorageManager backend = StorageManagerFactory.getInstance().getStorageManager(job.getUsername());

		HttpClient client = MyHttpConnectionPoolProvider.getHttpClient();
		InputStream fileInp = backend.getBytes(job.getTargetId().getNodePath());

		HttpPut put = new HttpPut(putFileUrl);
		
		Node node = NodeFactory.getInstance().getNode(job.getTargetId(), job.getUsername());
		
        put.setEntity(new InputStreamEntity(fileInp, node.getNodeInfo().getSize()));

        try {
        	HttpResponse response = client.execute(put);
        	response.getEntity().getContent().close();
        } catch(IOException ex) {
			put.abort();
        	ex.printStackTrace();
        	throw ex;
        } finally {
        	try {
				if(null != fileInp) fileInp.close();
			} catch (IOException e) {}
        }
	}
}
