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
package edu.jhu.pha.vospace.node;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.TokenBuffer;

import edu.jhu.pha.vospace.QueueConnector;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.jobs.JobsProcessorServlet;
import edu.jhu.pha.vosync.meta.VoSyncMetaStore;

public class DataNode extends Node implements Cloneable {
	private static final Logger logger = Logger.getLogger(DataNode.class);
	private static final MappingJsonFactory f = new MappingJsonFactory();
	
	/**
	 * Construct a Node from the byte array
	 * @param req The byte array containing the Node
	 */
	public DataNode(byte[] bytes, String username, VospaceId id) {
		super(bytes, username, id);
	}


	public DataNode(VospaceId id, String username){
		super(id, username);
	}
	
	@Override
	public Object export(String format, Detail detail) {
		if(format.equals("json-dropbox") || format.equals("json-dropbox-object")){
			TokenBuffer g = new TokenBuffer(null);
			try {
	        	g.writeStartObject();

				g.writeStringField("size", readableFileSize(getNodeInfo().getSize()));
				g.writeNumberField("rev", getNodeInfo().getRevision());
				g.writeBooleanField("thumb_exists", false);
				g.writeNumberField("bytes", getNodeInfo().getSize());
				g.writeStringField("modified", dropboxDateFormat.format(getNodeInfo().getMtime()));
				g.writeStringField("path", getUri().getNodePath().getNodeOuterPath());
				g.writeBooleanField("is_dir", false);
				g.writeStringField("icon", "file");
				g.writeStringField("root", (getUri().getNodePath().isEnableAppContainer()?"sandbox":"dropbox"));
				g.writeStringField("mime_type", getNodeInfo().getContentType());
	        	
	        	g.writeEndObject();
		    	g.close(); // important: will force flushing of output, close underlying output stream
			} catch (JsonGenerationException e) {
				e.printStackTrace();
				throw new InternalServerErrorException("Error generationg JSON: "+e.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
				throw new InternalServerErrorException("Error generationg JSON: "+e.getMessage());
			}
			
			if(format.equals("json-dropbox")) {
				try {
			    	ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
					JsonGenerator g2 = f.createJsonGenerator(byteOut); //.useDefaultPrettyPrinter() - doesn't work with metadata header
					g.serialize(g2);
					g2.close();
					byteOut.close();
	
					return byteOut.toByteArray();
				} catch (IOException e1) { // shouldn't happen
					logger.error("Error creating JSON generator: "+e1.getMessage());
					throw new InternalServerErrorException("Error creating JSON generator: "+e1.getMessage());
				}

			} else {
				try {
					return g.asParser(f.getCodec()).readValueAsTree();
				} catch (JsonProcessingException e) {
					logger.error("Error generating JSON: "+e.getMessage());
					throw new InternalServerErrorException("Error generating JSON: "+e.getMessage());
				} catch (IOException e) {
					logger.error("Error generating JSON: "+e.getMessage());
					throw new InternalServerErrorException("Error generating JSON: "+e.getMessage());
				}
			}
		} else {
			return getXmlMetadata(detail).getBytes();
		}
	}

	/**
	 * Set the node content
	 * @param data The new node content
	 */
	public void setData(InputStream data) {
		if(!getMetastore().isStored(getUri()))
			throw new NotFoundException("NodeNotFound");
		logger.debug("Updating node "+getUri().toString());

		// put the node data into storage
		getStorage().putBytes(getUri().getNodePath(), data);
		
		// update node size from storage to metadata
		getStorage().updateNodeInfo(getUri().getNodePath(), getNodeInfo());
		
		getNodeInfo().setRevision(getNodeInfo().getRevision()+1);//increase revision version to store in DB
		
		getMetastore().storeInfo(getUri(), getNodeInfo());
		
		try {
			// Update root container size
			ContainerNode contNode = (ContainerNode)NodeFactory.getNode(
					new VospaceId(new NodePath(getUri().getNodePath().getContainerName())), 
					this.getOwner());
			getStorage().updateNodeInfo(contNode.getUri().getNodePath(), contNode.getNodeInfo());
			getMetastore().storeInfo(contNode.getUri(), contNode.getNodeInfo());
			//logger.debug("Updated node "+contNode.getUri().toString()+" size to: "+contNode.getNodeInfo().getSize());
		} catch (URISyntaxException e) {
			logger.error("Updating root node size failed: "+e.getMessage());
		}
		
		QueueConnector.goAMQP("setData", new QueueConnector.AMQPWorker<Boolean>() {
			@Override
			public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

				channel.exchangeDeclare(conf.getString("vospace.exchange.nodechanged"), "fanout", false);
				channel.exchangeDeclare(conf.getString("process.exchange.nodeprocess"), "fanout", true);

				Map<String,Object> nodeData = new HashMap<String,Object>();
				nodeData.put("uri",getUri().toString());
				nodeData.put("owner",getOwner());
    			nodeData.put("container", getUri().getNodePath().getParentPath().getNodeStoragePath());

    			byte[] jobSer = (new ObjectMapper()).writeValueAsBytes(nodeData);
    			channel.basicPublish(conf.getString("vospace.exchange.nodechanged"), "", null, jobSer);
				channel.basicPublish(conf.getString("process.exchange.nodeprocess"), "", null, jobSer);
		    	
		    	return true;
			}
		});

	}

	@Override
	public NodeType getType() {
		return NodeType.DATA_NODE;
	}


	public void setChunkedData(String uploadId) {
		if(!getMetastore().isStored(getUri()))
			throw new NotFoundException("NodeNotFound");
		logger.debug("Updating chunked node "+getUri().toString());
		
		VoSyncMetaStore vosyncMeta = new VoSyncMetaStore(this.owner);

		// put the node data into storage
		getStorage().putChunkedBytes(getUri().getNodePath(), uploadId);

		vosyncMeta.mapChunkedToNode(this.getUri(), uploadId);
		
		// update node size from storage to metadata
		getStorage().updateNodeInfo(getUri().getNodePath(), getNodeInfo());
		
		getNodeInfo().setRevision(getNodeInfo().getRevision()+1);//increase revision version to store in DB
		
		getMetastore().storeInfo(getUri(), getNodeInfo());
		
		try {
			// Update root container size
			ContainerNode contNode = (ContainerNode)NodeFactory.getNode(
					new VospaceId(new NodePath(getUri().getNodePath().getContainerName())), 
					this.getOwner());
			getStorage().updateNodeInfo(contNode.getUri().getNodePath(), contNode.getNodeInfo());
			getMetastore().storeInfo(contNode.getUri(), contNode.getNodeInfo());
			//logger.debug("Updated node "+contNode.getUri().toString()+" size to: "+contNode.getNodeInfo().getSize());
		} catch (URISyntaxException e) {
			logger.error("Updating root node size failed: "+e.getMessage());
		}
		
		QueueConnector.goAMQP("setData", new QueueConnector.AMQPWorker<Boolean>() {
			@Override
			public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

				channel.exchangeDeclare(conf.getString("vospace.exchange.nodechanged"), "fanout", false);
				channel.exchangeDeclare(conf.getString("process.exchange.nodeprocess"), "fanout", true);

				Map<String,Object> nodeData = new HashMap<String,Object>();
				nodeData.put("uri",getUri().toString());
				nodeData.put("owner",getOwner());
    			nodeData.put("container", getUri().getNodePath().getParentPath().getNodeStoragePath());

    			byte[] jobSer = (new ObjectMapper()).writeValueAsBytes(nodeData);
    			channel.basicPublish(conf.getString("vospace.exchange.nodechanged"), "", null, jobSer);
				channel.basicPublish(conf.getString("process.exchange.nodeprocess"), "", null, jobSer);
		    	
		    	return true;
			}
		});

	}

}
