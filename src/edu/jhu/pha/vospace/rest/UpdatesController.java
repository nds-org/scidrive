package edu.jhu.pha.vospace.rest;

import java.io.IOException;

import javax.annotation.security.PermitAll;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseFeature;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.QueueingConsumer;

import edu.jhu.pha.vospace.QueueConnector;
import edu.jhu.pha.vospace.SettingsServlet;

/**
 * Provides the REST service for /updates/ path: retrieving realtime updates using SSE
 * @author Dmitry Mishin
 */
@Path("/updates/")

public class UpdatesController {
	private static final Logger logger = Logger.getLogger(UpdatesController.class);
	static Configuration conf = SettingsServlet.getConfig();
	
	@GET
	@PermitAll
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    public EventOutput getServerSentEvents(@Suspended final AsyncResponse asyncResponse) {
        final EventOutput eventOutput = new EventOutput();
        new Thread() {
            @Override
            public void run() {
            	final Thread listenThread = this;
                try {
            		QueueConnector.goAMQP("getUpdates", new QueueConnector.AMQPWorker<Boolean>() {
            			@Override
            			public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

            				channel.exchangeDeclarePassive(conf.getString("vospace.exchange.nodechanged"));

            				DeclareOk listenQueue = channel.queueDeclare();
            				
            				channel.queueBind(listenQueue.getQueue(), conf.getString("vospace.exchange.nodechanged"), "*");

            				QueueingConsumer consumer = new QueueingConsumer(channel);
            				channel.basicConsume(listenQueue.getQueue(), true, consumer);

        					while (!listenThread.isInterrupted()) {
        						try {
	    					    	QueueingConsumer.Delivery delivery = consumer.nextDelivery();
	    					    	
	    					    	// Job JSON notation
	    					    	byte[] message = delivery.getBody();
	    						    JsonNode update = (new ObjectMapper()).readValue(message, 0, message.length, JsonNode.class);
	    						    String container = update.path("container").getTextValue();
	    						    
	        	                	final OutboundEvent.Builder eventBuilder = new OutboundEvent.Builder();
	        	                    eventBuilder.id(Long.toString(System.currentTimeMillis()));
	        	                    eventBuilder.data(String.class, new String(message));
	        	                    final OutboundEvent event = eventBuilder.build();
	        	                    eventOutput.write(event);
        						} catch(InterruptedException ex) {
        							logger.error("Error reading update message from queue: "+ex.getMessage());
        						}
        					}
            		    	return true;
            			}
            		});	
                } finally {
                    try {
                        eventOutput.close();
                    } catch (IOException ioClose) {
                        throw new RuntimeException(
                            "Error when closing the event output.", ioClose);
                    }
                }
            }
        }.start();
        return eventOutput;
    }
}
