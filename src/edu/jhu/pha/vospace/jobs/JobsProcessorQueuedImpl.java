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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.rabbitmq.client.QueueingConsumer;

import edu.jhu.pha.vospace.QueueConnector;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.protocol.ProtocolHandler;
import edu.jhu.pha.vospace.rest.JobDescription;
import edu.jhu.pha.vospace.rest.JobDescription.DIRECTION;

/**
 * The JobsProcessor class creates a separate thread, waiting for new jobs to appear in the queue.
 * If a new client-initialized job appears, the process sets the needed fields of the job and sets the finished status.
 * @author Dmitry Mishin
 */
public class JobsProcessorQueuedImpl extends JobsProcessor {

	private static final long serialVersionUID = -9221962022096928143L;
	private static final Logger logger = Logger.getLogger(JobsProcessorQueuedImpl.class);
    private ExecutorService service;
	private Thread jobsThread = null;
	private static Vector<Future> workers;
	
	public JobsProcessorQueuedImpl() {
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
				logger.debug("Found the protocol handler "+protocolUri+": "+handlerInst);
				return handlerInst;
			} catch(InvocationTargetException e) {
				logger.error(e);
				throw new InternalServerErrorException(e.getMessage());
			} catch (IllegalArgumentException e) {
				logger.error(e);
				throw new InternalServerErrorException(e.getMessage());
			} catch (SecurityException e) {
				logger.error(e);
				throw new InternalServerErrorException(e.getMessage());
			} catch (InstantiationException e) {
				logger.error(e);
				throw new InternalServerErrorException(e.getMessage());
			} catch (IllegalAccessException e) {
				logger.error(e);
				throw new InternalServerErrorException(e.getMessage());
			} catch (NoSuchMethodException e) {
				logger.error(e);
				throw new InternalServerErrorException(e.getMessage());
			}
		} else {
			logger.error("Not found the protocol handler "+protocolUri);
			return null;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		final JobsProcessor parentProc = this;
		QueueConnector.goAMQP("submitJob", new QueueConnector.AMQPWorker<Boolean>() {
			@Override
			public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

				channel.exchangeDeclare(conf.getString("transfers.exchange.submited"), "topic", true);

				channel.queueDeclare(conf.getString("transfers.queue.submited.server_initialised"), true, false, false, null);
				
				channel.queueBind(conf.getString("transfers.queue.submited.server_initialised"), conf.getString("transfers.exchange.submited"), "direction."+JobDescription.DIRECTION.PUSHFROMVOSPACE);
				channel.queueBind(conf.getString("transfers.queue.submited.server_initialised"), conf.getString("transfers.exchange.submited"), "direction."+JobDescription.DIRECTION.PULLTOVOSPACE);
				channel.queueBind(conf.getString("transfers.queue.submited.server_initialised"), conf.getString("transfers.exchange.submited"), "direction."+JobDescription.DIRECTION.LOCAL);

				QueueingConsumer consumer = new QueueingConsumer(channel);
				channel.basicConsume(conf.getString("transfers.queue.submited.server_initialised"), false, consumer);

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
					    	logger.debug("Waiting for a job");
					    	QueueingConsumer.Delivery delivery = consumer.nextDelivery();
					    	
					    	// Job JSON notation
						    JobDescription job = (new ObjectMapper()).readValue(delivery.getBody(), 0, delivery.getBody().length, JobDescription.class);
						    
					    	logger.debug("There's a submited job! "+job.getId());
	
			    			TransferThread thread = new TransferThread(job, parentProc);
			    			thread.setName(job.getId());
			    			
			    			Future future = service.submit(thread);
			    			
			    			workers.add(future);
			    			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
						}
					}
				} catch (InterruptedException ex) {
					// server restarted
				}
		    	
		    	return true;
			}
		});				
		
		
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
	
	public static void submitJob(final String login, final JobDescription job) {
		JobsProcessor.submitJob(login, job);
		if(job.getDirection() == DIRECTION.PUSHFROMVOSPACE || job.getDirection() == DIRECTION.PULLTOVOSPACE || job.getDirection() == DIRECTION.LOCAL) {
			QueueConnector.goAMQP("submitJob", new QueueConnector.AMQPWorker<Boolean>() {
				@Override
				public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

					channel.exchangeDeclare(conf.getString("transfers.exchange.submited"), "topic", true);
					
					byte[] jobSer = (new ObjectMapper()).writeValueAsBytes(job);
					channel.basicPublish(conf.getString("transfers.exchange.submited"), "direction."+job.getDirection(), null, jobSer);
			    	
			    	return true;
				}
			});				
		}
	}
	
}
