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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.QueueConnector;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.jobs.JobsProcessor;
import edu.jhu.pha.vospace.rest.JobDescription;
import edu.jhu.pha.vospace.rest.JobDescription.DIRECTION;
import edu.jhu.pha.vosync.exception.BadRequestException;
import edu.jhu.pha.vosync.exception.InternalServerErrorException;


public class SimulationProcessor {

	private static Configuration conf = SettingsServlet.getConfig();
	private static Logger logger = Logger.getLogger(SimulationProcessor.class);
	private final static String SIM_EXCHANGE = "sdsc.vobox.simulation";
    
	public static void processNodeMeta(Metadata metadata, Object handler, JsonNode credentials) throws Exception {
        String owner = metadata.get("owner");
        String source = metadata.get(TikaCoreProperties.SOURCE);
        logger.debug("!!!!!Found simulation: "+owner+" "+source);
        
		JobDescription job = new JobDescription();
		job.setTarget(source);

		job.setDirection(DIRECTION.PULLFROMVOSPACE);
		job.setId(UUID.randomUUID().toString());
		job.setStartTime(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime());
		job.setState(JobDescription.STATE.PENDING);
		job.addProtocol("ivo://ivoa.net/vospace/core#httpget", null);
		job.setUsername(owner);
		
		Method submitJobMethod;
		try {
			submitJobMethod = JobsProcessor.getImplClass().getMethod("submitJob", String.class, JobDescription.class);
			submitJobMethod.invoke(null, owner, job);
			
			final String simEndpointUrl = conf.getString("application.url")+"/data/"+job.getId();
			
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
			logger.error("Error creating simulation endpoint: "+e.getMessage());
		}
  	}

}

