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
package edu.sdsc.vospace.process;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.QueueConnector;
import edu.jhu.pha.vospace.node.DataNode;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.process.tika.SimulationParser;


public class SimulationProcessor {

	private static Logger logger = Logger.getLogger(SimulationProcessor.class);
	private final static String SIM_EXCHANGE = "sdsc.vobox.simulation";
    
	private final static String SIM_ID_PROPERTY = "ivo://ivoa.net/vospace/core#simulation_id";
	private final static String SIM_DATASET_ID_PROPERTY = "ivo://ivoa.net/vospace/core#simulation_dataset";
	
	public static void processNodeMeta(Metadata metadata, Object handler, JsonNode credentials) throws Exception {
        String owner = metadata.get("owner");
        String source = metadata.get(TikaCoreProperties.SOURCE);

        DataNode node = (DataNode)NodeFactory.getNode(new VospaceId(source), owner);
        
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(SIM_ID_PROPERTY, metadata.get(SimulationParser.METADATA_SIMULATION_UUID));
        properties.put(SIM_DATASET_ID_PROPERTY, 
        		StringUtils.join(metadata.getValues(SimulationParser.METADATA_DATASET_UUID), " "));
        node.getMetastore().updateUserProperties(node.getUri(), properties);
        
		try {
			final String simEndpointUrl = node.getHttpDownloadLink().toASCIIString();
			
			QueueConnector.goAMQP("submit new simulation", new QueueConnector.AMQPWorker<Boolean>() {
				@Override
				public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {
					Map<String,Object> jobData = new HashMap<String,Object>();
					jobData.put("url",simEndpointUrl);

	    			byte[] jobSer = (new ObjectMapper()).writeValueAsBytes(jobData);

					channel.exchangeDeclare(SIM_EXCHANGE, "topic", true);
					channel.basicPublish(SIM_EXCHANGE, "new_sim", null, jobSer);
			    	
			    	return true;
				}
			});				
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error creating simulation endpoint: "+e.getMessage());
		}
  	}

}

