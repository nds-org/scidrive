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

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.generationjava.io.xml.SimpleXmlWriter;
import com.ximpleware.AutoPilot;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import com.ximpleware.XPathParseException;

import edu.jhu.pha.vospace.QueueConnector;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.BadRequestException;
import edu.jhu.pha.vospace.api.exceptions.ForbiddenException;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.jobs.JobsProcessorServlet;
import edu.jhu.pha.vospace.meta.MetaStore;
import edu.jhu.pha.vospace.meta.MetaStoreFactory;
import edu.jhu.pha.vospace.meta.NodesList;
import edu.jhu.pha.vospace.rest.TransfersController;
import edu.jhu.pha.vospace.storage.StorageManager;
import edu.jhu.pha.vospace.storage.StorageManagerFactory;

public abstract class Node implements Cloneable {
	public enum Detail {min, max, properties};
	public enum PropertyType {property, accepts, provides, contains};
	private final String VOS_NAMESPACE = "http://www.ivoa.net/xml/VOSpace/v2.0";
	private final String XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance";
	
	MetaStore _metastore = null;
	StorageManager _storage = null;
	
	String owner;
	VospaceId id;
	NodeInfo nodeInfo;
	XMLObject nodeXML;
	
	private Map<String, String> properties = null;
	
	private static final Logger logger = Logger.getLogger(Node.class);
	static final SimpleDateFormat dropboxDateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z");
	static Configuration conf = SettingsServlet.getConfig();
	
	public static String readableFileSize(long size) {
	    if(size <= 0) return "0 B";
	    final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
	    int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
	    return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
	}

	public Node(byte[] bytes, String username, VospaceId id) {
		this.owner = username;
		this.nodeXML = new XMLObject(bytes);
		setUri(id);
	}

	/**
	 * Construct a Node from the byte array
	 * @param req The byte array containing the Node
	 */
	public Node(VospaceId id, String username) {
		this.owner = username;
		this.id = id;
	}

	public void copy(VospaceId newLocationId) {
		if(!isStoredMetadata())
			throw new NotFoundException("NodeNotFound");
		
		if(getMetastore().isStored(newLocationId))
			throw new ForbiddenException("DestinationNodeExists");

		getStorage().copyBytes(getUri().getNodePath(), newLocationId.getNodePath());

		final Node newDataNode = NodeFactory.getInstance().createNode(newLocationId, owner, this.getType());
		newDataNode.setNode(null);
		newDataNode.getStorage().updateNodeInfo(newLocationId.getNodePath(), newDataNode.getNodeInfo());
		newDataNode.getMetastore().storeInfo(newLocationId, newDataNode.getNodeInfo());
		newDataNode.getMetastore().updateUserProperties(newLocationId, getNodeMeta(PropertyType.property));

		QueueConnector.goAMQP("copyNode", new QueueConnector.AMQPWorker<Boolean>() {
			@Override
			public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

				channel.exchangeDeclare(conf.getString("vospace.exchange.nodechanged"), "fanout", false);
				channel.exchangeDeclare(conf.getString("process.exchange.nodeprocess"), "fanout", true);

				Map<String,Object> nodeData = new HashMap<String,Object>();
				nodeData.put("uri", newDataNode.getUri().toString());
				nodeData.put("owner",getOwner());
    			nodeData.put("container", newDataNode.getUri().getNodePath().getParentPath().getNodeStoragePath());

    			byte[] jobSer = (new ObjectMapper()).writeValueAsBytes(nodeData);
    			channel.basicPublish(conf.getString("vospace.exchange.nodechanged"), "", null, jobSer);
				channel.basicPublish(conf.getString("process.exchange.nodeprocess"), "", null, jobSer);
		    	
		    	return true;
			}
		});
	}

	public void createParent() {
		if(!getUri().getNodePath().isRoot(true)) {
			if(!hasValidParent()) {
				try {
					Node parentNode = NodeFactory.getInstance().createNode(getUri().getParent(), owner, NodeType.CONTAINER_NODE);
					parentNode.createParent();
					parentNode.setNode(null);
				} catch(URISyntaxException ex) {
					throw new BadRequestException("InvalidURI");
				}
			}
		}
	}
	
	/**
	 * @return Node download transfer endpoint
	 */
	public String downloadNode() {
		StringWriter jobWriter = new StringWriter();
		SimpleXmlWriter xw = new SimpleXmlWriter(jobWriter);
		try {
			xw.setDefaultNamespace("vos");
			xw.writeEntity("transfer");
			xw.writeAttribute("xmlns:vos", "http://www.ivoa.net/xml/VOSpace/v2.0");
			
			xw.writeEntityWithText("target",this.getUri());
			xw.writeEntityWithText("direction","pullFromVoSpace");
			xw.writeEntityWithText("view","ivo://ivoa.net/vospace/core#defaultview");
			xw.writeEntity("protocol").writeAttribute("uri", "ivo://ivoa.net/vospace/core#httpget").endEntity();
			
			xw.endEntity();
			xw.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
		
		UUID jobId = TransfersController.submitJob(jobWriter.toString(), owner);
		
		String jobUrl = SettingsServlet.getConfig().getString("application.url")+"/transfers/"+jobId.toString();
		
		String dataEndpoint = null;
		
		VTDGen vg = new VTDGen();
		vg.parseHttpUrl(jobUrl,true);
		
		AutoPilot ap = new AutoPilot();
		ap.declareXPathNameSpace("vos", "http://www.ivoa.net/xml/VOSpace/v2.0");
		ap.declareXPathNameSpace("uws", "http://www.ivoa.net/xml/UWS/v1.0");

		VTDNav vn = vg.getNav();
		ap.bind(vn);
		
		try {
			ap.selectXPath("//vos:protocol[@uri='ivo://ivoa.net/vospace/core#httpget']/vos:protocolEndpoint");
			dataEndpoint = ap.evalXPathToString();
			ap.resetXPath();
		} catch(XPathParseException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
		return dataEndpoint;
	}
	
	public abstract Object export(String format, Detail detail);
	
	public InputStream exportData() {
		return getStorage().getBytes(this.getUri().getNodePath());
	}
	
	public MetaStore getMetastore() {
		if(null == this._metastore)
			this._metastore = MetaStoreFactory.getInstance().getMetaStore(owner);
		return _metastore;
	}

	public NodeInfo getNodeInfo() {
		if(null == this.nodeInfo) {
			NodeInfo nodeInfo = getMetastore().getNodeInfo(getUri());
			this.nodeInfo = nodeInfo;
		}
		
		return this.nodeInfo;
	}

	public String getXmlMetadata(Detail detail) {
		
		StringWriter jobWriter = new StringWriter();
		try {
			XMLStreamWriter xsw = XMLOutputFactory.newInstance().createXMLStreamWriter(jobWriter);
		    xsw.writeStartDocument();

		    xsw.setDefaultNamespace(VOS_NAMESPACE);
		    
		    xsw.writeStartElement("node");
			xsw.writeNamespace("xsi", XSI_NAMESPACE);
			xsw.writeNamespace(null, VOS_NAMESPACE);
			xsw.writeAttribute("xsi:type", this.getType().getTypeName());
			xsw.writeAttribute("uri", this.getUri().toString());

			if(detail == Detail.max) {
			    xsw.writeStartElement("properties");
					Map<String, String> properties = this.getMetastore().getProperties(this.getUri(), PropertyType.property);
					properties.put(conf.getString("core.property.length"), Long.toString(getNodeInfo().getSize()));
					properties.put(conf.getString("core.property.date"), dropboxDateFormat.format(getNodeInfo().getMtime()));
					if(this.getType() == NodeType.DATA_NODE) {
						properties.put(conf.getString("core.property.contenttype"), getNodeInfo().getContentType());
					}
					
					for(String propUri: properties.keySet()) {
					    xsw.writeStartElement("property");
					    xsw.writeAttribute("uri", propUri);
					    xsw.writeCharacters(properties.get(propUri));
					    xsw.writeEndElement();
					}

				    
				xsw.writeEndElement();
	
			    xsw.writeStartElement("accepts");
				xsw.writeEndElement();
	
			    xsw.writeStartElement("provides");
				xsw.writeEndElement();
	
			    xsw.writeStartElement("capabilities");
				xsw.writeEndElement();
				
				if(this.getType() == NodeType.CONTAINER_NODE) {
					NodesList childrenList = ((ContainerNode)this).getDirectChildren(false, 0, -1);
					List<Node> children = childrenList.getNodesList();

				    xsw.writeStartElement("nodes");
					for(Node childNode: children) {
					    xsw.writeStartElement("node");
					    xsw.writeAttribute("uri", childNode.getUri().getId().toString());
					    xsw.writeEndElement();
					}
					xsw.writeEndElement();

				}
				
			}

			xsw.writeEndElement();
		    
		    xsw.writeEndDocument();
		    xsw.close();
		} catch (XMLStreamException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
		return jobWriter.getBuffer().toString();
	}
	
	public Map<String, String> getNodeMeta(PropertyType type) {
		switch(type) {
		case property:
			if(null == this.properties)
				this.properties = this.getMetastore().getProperties(this.getUri(), type.property);
			return this.properties;
		default:
			return null;
		}
	}
	
	public String getOwner() {
		return owner;
	}

	public Node getParent() {
		VospaceId parentId;
		try {
			parentId = this.getUri().getParent();
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}
		Node parent = NodeFactory.getInstance().getNode(parentId, owner);
		
		return parent;
	}

	public StorageManager getStorage() {
		if(null == this._storage)
			this._storage = StorageManagerFactory.getInstance().getStorageManager(owner);
		return _storage;
	}

	/**
	 * Get the type of the node
	 * @return The type of the node
	 */
	public abstract NodeType getType();
	
	/**
	 * Get the uri of the node
	 * @return The uri of the node
	 */
	public VospaceId getUri() {
		return this.id;
	}
	
	/**
	 * Check whether the parent node of the specified identifier is valid:
	 *   - it exists
	 *   - it is a container
	 */
	public boolean hasValidParent() {
		VospaceId parent;
		try {
			parent = getUri().getParent();
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}
		
		if(parent.getNodePath().isRoot(false))
			return true;

		if(!getMetastore().isStored(parent)) {
			logger.debug(parent.getId().toASCIIString()+" is not stored");
			return false;
		}
		
		return getMetastore().getType(parent) == NodeType.CONTAINER_NODE;
	}

	/**
	 * Checks if the node exists in the metadata database
	 * @return
	 */
	public boolean isStoredMetadata() {
		return getMetastore().isStored(this.getUri());
	}
	
	/**
	 * Marks the node as removed in metadata database
	 */
	public void markRemoved() {
		if(!isStoredMetadata())
			throw new NotFoundException("NodeNotFound");
		
		getMetastore().markRemoved(getUri());

		QueueConnector.goAMQP("markRemovedNode", new QueueConnector.AMQPWorker<Boolean>() {
			@Override
			public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

				channel.exchangeDeclare(conf.getString("vospace.exchange.nodechanged"), "fanout", false);

				Map<String,Object> nodeData = new HashMap<String,Object>();
				nodeData.put("uri",getUri().toString());
				nodeData.put("owner",getOwner());
    			nodeData.put("container", getUri().getNodePath().getParentPath().getNodeStoragePath());

    			byte[] jobSer = (new ObjectMapper()).writeValueAsBytes(nodeData);
    			channel.basicPublish(conf.getString("vospace.exchange.nodechanged"), "", null, jobSer);
		    	
		    	return true;
			}
		});
	}
	
	/**
	 * Move the node to new location
	 * @param newLocationId
	 */
	//TODO move operation with chunked files
	public void move(VospaceId newLocationId) {
		if(!isStoredMetadata())
			throw new NotFoundException("NodeNotFound");
		
		if(getMetastore().isStored(newLocationId))
			throw new ForbiddenException("DestinationNodeExists");

		getStorage().copyBytes(getUri().getNodePath(), newLocationId.getNodePath());

		final Node newDataNode = NodeFactory.createNode(newLocationId, owner, this.getType());
		newDataNode.setNode(null);
		newDataNode.getStorage().updateNodeInfo(newLocationId.getNodePath(), newDataNode.getNodeInfo());
		newDataNode.getMetastore().storeInfo(newLocationId, newDataNode.getNodeInfo());
		newDataNode.getMetastore().updateUserProperties(newLocationId, getNodeMeta(PropertyType.property));

		this.markRemoved();
		
		QueueConnector.goAMQP("copyNode", new QueueConnector.AMQPWorker<Boolean>() {
			@Override
			public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

				channel.exchangeDeclare(conf.getString("vospace.exchange.nodechanged"), "fanout", false);
				channel.exchangeDeclare(conf.getString("process.exchange.nodeprocess"), "fanout", true);

				Map<String,Object> nodeData = new HashMap<String,Object>();
				nodeData.put("uri", newDataNode.getUri().toString());
				nodeData.put("owner",getOwner());
    			nodeData.put("container", newDataNode.getUri().getNodePath().getParentPath().getNodeStoragePath());

    			byte[] jobSer = (new ObjectMapper()).writeValueAsBytes(nodeData);
    			channel.basicPublish(conf.getString("vospace.exchange.nodechanged"), "", null, jobSer);
				channel.basicPublish(conf.getString("process.exchange.nodeprocess"), "", null, jobSer);
		    	
				
				
				Map<String,Object> oldNodeData = new HashMap<String,Object>();
				oldNodeData.put("uri",getUri().toString());
				oldNodeData.put("owner",getOwner());
				oldNodeData.put("container", getUri().getNodePath().getParentPath().getNodeStoragePath());

    			byte[] oldNodejobSer = (new ObjectMapper()).writeValueAsBytes(oldNodeData);
    			channel.basicPublish(conf.getString("vospace.exchange.nodechanged"), "", null, oldNodejobSer);
		    	return true;
			}
		});
	}

	/**
	 * Set node XML metadata and create storage empty node if needed 
	 */
	public void setNode(String nodeXml) {
		if (!isStoredMetadata()) {
			if(getType().equals(NodeType.CONTAINER_NODE))
				getStorage().createContainer(this.getUri().getNodePath());
			else
				if(this.getUri().getNodePath().getNodeRelativeStoragePath().isEmpty()) // creating non-container in first level
					throw new BadRequestException("BadRequest");
			getMetastore().storeData(getUri(), this.getType());
		}

		if(null != nodeXml) {
			XMLObject nodeXmlObj = new XMLObject(nodeXml.getBytes());
			getMetastore().updateUserProperties(getUri(), nodeXmlObj.getNodeProperties());
		}
		
		QueueConnector.goAMQP("setNode", new QueueConnector.AMQPWorker<Boolean>() {
			@Override
			public Boolean go(com.rabbitmq.client.Connection conn, com.rabbitmq.client.Channel channel) throws IOException {

				channel.exchangeDeclare(conf.getString("vospace.exchange.nodechanged"), "fanout", false);

				Map<String,Object> nodeData = new HashMap<String,Object>();
				nodeData.put("uri",getUri().toString());
				nodeData.put("owner",getOwner());
    			nodeData.put("container", getUri().getNodePath().getParentPath().getNodeStoragePath());

    			byte[] jobSer = (new ObjectMapper()).writeValueAsBytes(nodeData);
    			channel.basicPublish(conf.getString("vospace.exchange.nodechanged"), "", null, jobSer);
		    	
		    	return true;
			}
		});

	}

	public void setNodeInfo(NodeInfo nodeInfo) {
		this.nodeInfo = nodeInfo;
	}
	
	/**
	 * Set the uri of the node
	 * @param uri The new uri of the node
	 */
	public void setUri(VospaceId uri) {
		this.id = uri;
	}
	
	public String toString() {
		assert(false):"Not used";
		return "";
	}

}
