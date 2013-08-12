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
import java.net.InetAddress;
import java.text.NumberFormat;

import org.apache.log4j.Logger;

import udt.UDTClient;
import udt.UDTOutputStream;
import udt.util.Util;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.jobs.JobException;
import edu.jhu.pha.vospace.jobs.JobsProcessor;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.rest.JobDescription;
import edu.jhu.pha.vospace.rest.JobDescription.STATE;
import edu.jhu.pha.vospace.storage.StorageManager;
import edu.jhu.pha.vospace.storage.StorageManagerFactory;

/**
 * This class handles the implementation details for the HTTP 1.1 PUT protocol
 */
public class UdtPutProtocolHandler implements ProtocolHandler {

	private static final Logger logger = Logger.getLogger(UdtPutProtocolHandler.class);

	private final NumberFormat format=NumberFormat.getNumberInstance();
	
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.protocol.ProtocolHandler#getUri()
	 */
	@Override
	public String getUri() {
    	return SettingsServlet.getConfig().getString("transfers.protocol.udtput");
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.protocol.ProtocolHandler#invoke(edu.jhu.pha.vospace.rest.JobDescription)
	 */
	@Override
    public void invoke(JobDescription job) throws IOException, JobException {
		logger.debug("UDT Put job invoked "+job.getId());
		StorageManager backend = StorageManagerFactory.getStorageManager(job.getUsername());

		InputStream inp = null;
		UDTOutputStream outp = null;

		InetAddress myHost = InetAddress.getLocalHost();
		UDTClient client= new UDTClient(myHost);

		try {
			String[] target = job.getProtocols().get(SettingsServlet.getConfig().getString("transfers.protocol.udtput")).split("[:/]");
			
			logger.debug(target[0]+" "+target[1]+" "+target[2]);
			
			client.connect(target[0], Integer.parseInt(target[1]));

			outp = client.getOutputStream();
			outp.write(target[2].getBytes());
			outp.flush();
			logger.debug("Sent the job id");

			Node node = NodeFactory.getNode(job.getTargetId(), job.getUsername());
			long size = node.getNodeInfo().getSize();
			
			outp.write(encode(size));
			outp.flush();
			
			long start=System.currentTimeMillis();

			Util.copy(backend.getBytes(job.getTargetId().getNodePath()), outp, size, false);
			JobsProcessor.modifyJobState(job, STATE.COMPLETED);

			logger.debug("[SendFile] Finished sending data.");
			long end=System.currentTimeMillis();
			double rate=1000.0*size/1024/1024/(end-start);
			logger.debug("[SendFile] Rate: "+format.format(rate)+" MBytes/sec. "+format.format(8*rate)+" MBit/sec.");
		} catch(InterruptedException ex) {
        	ex.printStackTrace();
        	throw new JobException(ex.getMessage());
        } finally {
			if(null != inp) try {inp.close();} catch(Exception ex) {}
			if(null != outp) try {outp.close();} catch(Exception ex) {}
			if(null != client) try {client.shutdown();} catch(Exception ex) {}
        }
	}
	
    static byte[]encode(long value){
        byte m4= (byte) (value>>24 );
        byte m3=(byte)(value>>16);
        byte m2=(byte)(value>>8);
        byte m1=(byte)(value);
        return new byte[]{m1,m2,m3,m4};
    }
}
