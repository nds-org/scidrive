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
package edu.jhu.pha.vospace.process;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Vector;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.CompositeDetector;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pkg.SimulationDetector;
import org.apache.tika.sax.BodyContentHandler;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.rabbitmq.client.QueueingConsumer;

import edu.jhu.pha.vospace.QueueConnector;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.node.ContainerNode;
import edu.jhu.pha.vospace.node.DataNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.oauth.UserHelper;

public class NodeProcessor implements Runnable {

	private static final Logger logger = Logger.getLogger(NodeProcessor.class);
	private final static String EXTERNAL_LINK_PROPERTY = "ivo://ivoa.net/vospace/core#external_link";
	private final static String PROCESSING_PROPERTY = "ivo://ivoa.net/vospace/core#processing";
	private final static String ERROR_MESSAGE_PROPERTY = "ivo://ivoa.net/vospace/core#error_message";

    static Configuration conf = SettingsServlet.getConfig();

	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    
	public final static Map<String, ProcessorConfig> processors = new HashMap<String, ProcessorConfig>();
	
	static {
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

		try	{
			XMLConfiguration processorConf = new XMLConfiguration("processors.xml");
			processorConf.setExpressionEngine(new XPathExpressionEngine());

			for(String processorId: processorConf.getStringArray("//processor/id")) {
				processors.put(processorId, new ProcessorConfig(processorConf, processorId));
			}
		} catch(ConfigurationException ex) {
		    logger.error("Error reading the nodes processor configuration file processors.xml." + ex.getMessage());
		}
	}
	
    @Override
	public void run() {
		QueueConnector.goAMQP("nodesProcessor", new QueueConnector.AMQPWorker<Boolean>() {
			@Override
			public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

				channel.exchangeDeclare(conf.getString("process.exchange.nodeprocess"), "fanout", true);
				channel.exchangeDeclare(conf.getString("vospace.exchange.nodechanged"), "fanout", false);

				channel.queueDeclare(conf.getString("process.queue.nodeprocess"), true, false, false, null);
				
				channel.queueBind(conf.getString("process.queue.nodeprocess"), conf.getString("process.exchange.nodeprocess"), "");

				QueueingConsumer consumer = new QueueingConsumer(channel);
				channel.basicConsume(conf.getString("process.queue.nodeprocess"), false, consumer);
				
				while (!Thread.currentThread().isInterrupted()) {

					Node node = null;
					
	            	try {
				    	QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				    	
				    	Map<String,Object> nodeData = (new ObjectMapper()).readValue(delivery.getBody(), 0, delivery.getBody().length, new TypeReference<HashMap<String,Object>>() {});

		            	channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
						node = NodeFactory.getNode(new VospaceId((String)nodeData.get("uri")), (String)nodeData.get("owner"));

		            	logger.debug("Node changed: "+nodeData.get("uri")+" "+nodeData.get("owner")+" "+node.getType());
		            	
		            	switch(node.getType()) {
			            	case DATA_NODE: {
			            		TikaInputStream inp = null;
			            		MediaType type = null;
			            		try {
			            			Metadata nodeTikaMeta = new Metadata();
			            			inp = TikaInputStream.get(node.exportData());
			            			//MediaType type = new DefaultDetector().detect(inp, nodeTikaMeta);
			            			List<Detector> list = new ArrayList<Detector>();
			            			list.add(new SimulationDetector());
			            			list.add(new DefaultDetector());
			            			Detector detector = new CompositeDetector(list);
			            			type = detector.detect(inp, nodeTikaMeta);
			            			
			            			nodeTikaMeta.set(Metadata.CONTENT_TYPE, type.toString());
				            		node.getNodeInfo().setContentType(nodeTikaMeta.get(HttpHeaders.CONTENT_TYPE));
				            		node.getMetastore().storeInfo(node.getUri(), node.getNodeInfo());
			            		} catch(Exception ex) {
			            			logger.error("Error detecting file type: "+ex.toString());
			            		} finally {
			            			try {inp.close();} catch(Exception ex) {};
			            		}
			            		
			            		try {
			            			
			            			JsonNode allCredentials = UserHelper.getProcessorCredentials(node.getOwner());
			            			
			            			List<String> externalLinks = new ArrayList<String>();
			            			for(Iterator<String> processorIds = allCredentials.getFieldNames(); processorIds.hasNext();) {
			            				String processorId = processorIds.next();
			            				ProcessorConfig processor = processors.get(processorId);
			            				JsonNode credentialsNode = allCredentials.findValue(processorId);
			            				
			            				String nodeContainer = node.getUri().getNodePath().getContainerName();
			            				
			            				JsonNode containersNode = credentialsNode.get("containers");
			            				boolean parseContainer = false;
			            				if(null != containersNode) {
			            					for(JsonNode curContainer: containersNode) {
			            						NodePath containerPath = new NodePath(curContainer.getTextValue());
			            						if(containerPath.getContainerName().equals(nodeContainer)) {
			            							parseContainer = true;
			            						}
			            					}
			            				}
			            				
			            				if(null != processor
			            						&& parseContainer
			            						&& processor.isSupportMimeType(node.getNodeInfo().getContentType())) {
				        					
						            		Metadata nodeTikaMeta = new Metadata();
						            		nodeTikaMeta.set(TikaCoreProperties.SOURCE,node.getUri().toString());
						            		nodeTikaMeta.set("owner",(String)nodeData.get("owner"));
						            		nodeTikaMeta.set(TikaCoreProperties.TITLE,node.getUri().getNodePath().getNodeName());
						            		nodeTikaMeta.add(TikaCoreProperties.METADATA_DATE,dateFormat.format(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime()));
				            				
						            		nodeTikaMeta.set(Metadata.CONTENT_LOCATION, ((DataNode)node).getHttpDownloadLink().toASCIIString());
						            		nodeTikaMeta.set(Metadata.CONTENT_TYPE, type.toString());
				            				
				            				AbstractParser parser;
			            					TikaConfig config = TikaConfig.getDefaultConfig();
				            				if(processor.getConfig() != null) {
				            					config = new TikaConfig(getClass().getResourceAsStream(processor.getConfig()));
				            				}
				            				
											parser = new CompositeParser(config.getMediaTypeRegistry(), config.getParser());
				            			
				            				ContentHandler handler;
				            				if(null != processor.getHandler())
				            					handler = (ContentHandler) Class.forName(processor.getHandler()).getConstructor().newInstance();
				            				else
				            					handler = new BodyContentHandler();
				            				
				            				InputStream str = null;
				            				try {
				            					str = TikaInputStream.get(node.exportData());
						            			parser.parse(str,
						            					handler,
						            			        nodeTikaMeta,
						            			        new ParseContext());
				            				} finally {
						            			try {str.close();} catch(Exception ex) {}
						            		}				            			
				            				
						            		// now do out-of-tika processing of results
						        			if(null != processor.getProcessor()) {
						        				try {
						        					Class handlerClass = Class.forName(processor.getProcessor());
						        					Method processMetaMethod = handlerClass.getMethod("processNodeMeta", Metadata.class, Object.class, JsonNode.class);
						        					
						        					processMetaMethod.invoke(handlerClass, nodeTikaMeta, handler, credentialsNode);
							        				logger.debug("Processing of "+node.getUri().toString()+" is finished.");
						        					
						        				} catch (Exception e) {
						        					logger.error("Error processing the node. "+e.getMessage());
						        					e.printStackTrace();
						        				}
						        			}
	
						        			String[] links = nodeTikaMeta.getValues("EXTERNAL_LINKS");
				        					
				        					if(null != links && links.length > 0) {
				        						externalLinks.addAll(Arrays.asList(links));
				        					}
			            				}
			            			}

			            			if(externalLinks.size() > 0) {
				            			Map<String, String> properties = new HashMap<String, String>();
			        			        properties.put(EXTERNAL_LINK_PROPERTY, StringUtils.join(externalLinks, ' '));
			        			        node.getMetastore().updateUserProperties(node.getUri(), properties);
			            			}

			            			Map<String, String> properties = new HashMap<String, String>();
		        			        properties.put(PROCESSING_PROPERTY, "done");
		        			        node.getMetastore().updateUserProperties(node.getUri(), properties);
		        			        
				            		logger.debug("Updated node "+node.getUri().toString()+" to "+node.getNodeInfo().getContentType()+" and "+node.getNodeInfo().getSize());

				            		// update node's container size metadata
				            		try {
				            			ContainerNode contNode = (ContainerNode)NodeFactory.getNode(
				            					new VospaceId(new NodePath(node.getUri().getNodePath().getContainerName())), 
				            					node.getOwner());
				            			node.getStorage().updateNodeInfo(contNode.getUri().getNodePath(), contNode.getNodeInfo());
				            			node.getMetastore().storeInfo(contNode.getUri(), contNode.getNodeInfo());
				            		} catch (URISyntaxException e) {
				            			logger.error("Updating root node size failed: "+e.getMessage());
				            		}
				            		
				            		try {
					        			nodeData.put("container", node.getUri().getNodePath().getParentPath().getNodeStoragePath());
					        			byte[] jobSer = (new ObjectMapper()).writeValueAsBytes(nodeData);
					        			channel.basicPublish(conf.getString("vospace.exchange.nodechanged"), "", null, jobSer);
					        		} catch (IOException e) {
					        			logger.error(e);
					        		}
			            		} catch(TikaException ex) {
			            			logger.error("Error parsing the node "+node.getUri().toString()+": "+ex.getMessage());
			            			processError(node, ex);
			            			ex.printStackTrace();
			            		} catch(SAXException ex) {
			            			logger.error("Error SAX parsing the node "+node.getUri().toString()+": "+ex.getMessage());
			            			processError(node, ex);
			            		} catch(IOException ex) {
			            			logger.error("Error reading the node "+node.getUri().toString()+": "+ex.getMessage());
			            			processError(node, ex);
			            		}
			            		
			            		break;
			            	}
			            	case CONTAINER_NODE: {
//				            	DbPoolServlet.goSql("Processing nodes",
//			                		"select * from nodes where owner = ?)",
//			                        new SqlWorker<Boolean>() {
//			                            @Override
//			                            public Boolean go(java.sql.Connection conn, java.sql.PreparedStatement stmt) throws SQLException {
//			                            	stmt.setString(1, node.getOwner());
//			                                /*ResultSet resSet = stmt.executeQuery();
//			                                while(resSet.next()) {
//			                                	String uriStr = resSet.getString(1);
//			                                	String username = resSet.getString(2);
//			                                	
//			                                	try {
//				                                	VospaceId uri = new VospaceId(uriStr);
//				                                	
//				                                	Node newNode = NodeFactory.getInstance().getNode(uri, username);
//				                                	newNode.remove();
//			                                	} catch(Exception ex) {
//			                                		ex.printStackTrace();
//			                                	}
//			                                }*/
//			                            	return true;
//			                            }
//			                        }
//			            	
//				                );
			            		break;
			            	}
			            	default: {
			            		break;
			            	}
		            	}
		            	
	            	} catch(InterruptedException ex) {
	            		logger.error("Sleeping interrupted. "+ex.getMessage());
            			processError(node, ex);
	            	} catch(JsonMappingException ex) {
	            		logger.error("Error reading the changed node JSON: "+ex.getMessage());
            			processError(node, ex);
	            	} catch(JsonParseException ex) {
	            		logger.error("Error reading the changed node JSON: "+ex.getMessage());
            			processError(node, ex);
	            	} catch (IOException ex) {
	            		ex.printStackTrace();
	            		logger.error("Error reading the changed node JSON: "+ex.getMessage());
            			processError(node, ex);
					} catch (URISyntaxException ex) {
	            		logger.error("Error parsing VospaceId from changed node JSON: "+ex.getMessage());
            			processError(node, ex);
					} catch (Exception ex) {
						ex.printStackTrace();
	            		logger.error("Error parsing fits: "+ex.getMessage());
            			processError(node, ex);
					}
	            }

		    	
		    	return true;
			}
		});

    }
    
    private void processError(Node node, Exception ex) {
    	if(null != node) {
    		try {
				Map<String, String> properties = new HashMap<String, String>();
		        properties.put(PROCESSING_PROPERTY, "error");
		        properties.put(ERROR_MESSAGE_PROPERTY, ex.getMessage());
		        node.getMetastore().updateUserProperties(node.getUri(), properties);
    		} catch(Exception ex2) {
    			logger.error("Error setting error node props: "+ex2.getMessage());
    		}
    	}
    }
    
    public static class ProcessorConfig {
    	private String id;
    	private List<String> mimeTypes = new Vector<String>();
    	private String config;
    	private String processor;
    	private String handler;
    	private String title;
    	private CredentialsSchema schema;

    	public ProcessorConfig(XMLConfiguration conf, String processorId) {
			this.id = processorId;
			for(String mimeType: conf.getStringArray("//processor[id='"+processorId+"']/mimetype")){
				mimeTypes.add(mimeType);
			}
			this.config = conf.getString("//processor[id='"+processorId+"']/config");
			this.processor = conf.getString("//processor[id='"+processorId+"']/processor");
			this.handler = conf.getString("//processor[id='"+processorId+"']/handler");
			this.title = conf.getString("//processor[id='"+processorId+"']/title", processorId);

			String[] namesArray = conf.getStringArray("//processor[id='"+processorId+"']/schema/field/@name");
			CredentialsSchema schema = new CredentialsSchema();
			schema.description = conf.getString("//processor[id='"+processorId+"']/description", "");

			if(namesArray != null && namesArray.length > 0){
				for(String fieldName: namesArray) {
					CredentialsSchemaField field = new CredentialsSchemaField();
					field.setName(fieldName);
					field.setRequired(conf.getBoolean("//processor[id='"+processorId+"']/schema/field[@name = '"+field.getName()+"']/@required"));
					field.setDefaultValue(conf.getString("//processor[id='"+processorId+"']/schema/field[@name = '"+field.getName()+"']/@default"));
					field.setPassword(conf.getBoolean("//processor[id='"+processorId+"']/schema/field[@name = '"+field.getName()+"']/@ispassword", false));
					schema.addField(field);
				}
			}
			this.schema = schema;
    	}
    	
    	public String getId() {
			return id;
		}
		public boolean isSupportMimeType(String mimeType) {
			return this.mimeTypes.contains(mimeType);
		}
		public String getConfig() {
			return config;
		}
		public String getProcessor() {
			return processor;
		}
		public String getHandler() {
			return handler;
		}
		public CredentialsSchema getSchema() {
			return schema;
		}
		public String getTitle() {
			return title;
		}
    }
    
    public static class CredentialsSchema {
    	private List<CredentialsSchemaField> properties = new Vector<CredentialsSchemaField>();
    	private String description;
    	public void addField(CredentialsSchemaField field) {
    		this.properties.add(field);
    	}
    	public List<CredentialsSchemaField> getFields() {
    		return properties;
    	}
    	@Override
		public String toString() {
			ObjectMapper mapper = new ObjectMapper();
			try {
				return mapper.writeValueAsString(this);
			} catch (Exception e) {
				e.printStackTrace();
				return "{}";
			}
    	}
		public String getDescription() {
			return description;
		}
    }
    
    public static class CredentialsSchemaField {
    	private String name;
    	private boolean required;
    	private String defaultValue = "";
    	private boolean isPassword = false;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public boolean isRequired() {
			return required;
		}
		public void setRequired(boolean required) {
			this.required = required;
		}
		public String getDefaultValue() {
			return defaultValue;
		}
		public void setDefaultValue(String defaultValue) {
			this.defaultValue = (null == defaultValue)?"":defaultValue;
		}
		public boolean isPassword() {
			return isPassword;
		}
		public void setPassword(boolean isPassword) {
			this.isPassword = isPassword;
		}
    }
    
}


