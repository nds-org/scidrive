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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;

import org.apache.log4j.Logger;

import udt.UDTClient;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.SizeLimitInputStream;
import edu.jhu.pha.vospace.jobs.JobException;
import edu.jhu.pha.vospace.rest.JobDescription;
import edu.jhu.pha.vospace.storage.StorageManager;
import edu.jhu.pha.vospace.storage.StorageManagerFactory;

/**
 * This class handles the implementation details for the HTTP 1.1 GET protocol
 */
public class UdtGetProtocolHandler implements ProtocolHandler {

	private static final Logger logger = Logger.getLogger(UdtGetProtocolHandler.class);
	
	/*
	 * (non-Javadoc)
	 * @see edu.caltech.vao.vospace.protocol.ProtocolHandler#getUri()
	 */
	@Override
	public String getUri() {
    	return SettingsServlet.getConfig().getString("transfers.protocol.udtget");
    }

    /*
     * (non-Javadoc)
     * @see edu.caltech.vao.vospace.protocol.ProtocolHandler#invoke(edu.jhu.pha.vospace.rest.JobDescription)
     */
	@Override
    public void invoke(JobDescription job) throws IOException, JobException{
		logger.debug("UDT Get job invoked "+job.getId());
		
		StorageManager backend = StorageManagerFactory.getStorageManager(job.getUsername());

		InputStream inp = null;
		
		try {
			InetAddress myHost = InetAddress.getLocalHost();
			UDTClient client= new UDTClient(myHost);
			
			String[] target = job.getProtocols().get(SettingsServlet.getConfig().getString("transfers.protocol.udtget")).split("[:/]");
			
			logger.debug(target[0]+" "+target[1]+" "+target[2]);
			
			client.connect(target[0], Integer.parseInt(target[1]));

			BufferedOutputStream outp = new BufferedOutputStream(client.getOutputStream());
			outp.write(target[2].getBytes());
			outp.flush();
			logger.debug("Sent the job id");
			inp = client.getInputStream();

			byte[]sizeInfo=new byte[8];
			int total=0;
			while(total<sizeInfo.length){
				int r=inp.read(sizeInfo);
				if(r<0)break;
				total+=r;
			}
			long size=decode(sizeInfo, 0);

			backend.putBytes(job.getTargetId().getNodePath(), new SizeLimitInputStream(inp, size));
			logger.debug("Got the file");
		} catch(IOException ex) {
			ex.printStackTrace();
			throw ex;
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new JobException(e.getMessage());
		} finally {
			logger.debug("Finishing the job "+job.getId());
			try {if(null != inp) inp.close();} catch (IOException e) {}
		}
    }

	static long decode(byte[]data, int start){
		long result = (data[start+3] & 0xFF)<<24
		             |(data[start+2] & 0xFF)<<16
					 |(data[start+1] & 0xFF)<<8
					 |(data[start] & 0xFF);
		return result;
	}
}
