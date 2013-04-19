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
package edu.jhu.pha.vospace.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URISyntaxException;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import nu.xom.Builder;
import nu.xom.Serializer;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.BadRequestException;
import edu.jhu.pha.vospace.api.exceptions.ConflictException;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.node.ContainerNode;
import edu.jhu.pha.vospace.node.DataNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.Node.Detail;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.StructuredDataNode;
import edu.jhu.pha.vospace.node.UnstructuredDataNode;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.oauth.VoboxUser;

/**
 * Provides the REST service for /nodes/ path: the functions for manipulating the nodes content and metadata
 * @author Dmitry Mishin
 */
@Path("/nodes/")
public class NodesController {
	private static final Logger logger = Logger.getLogger(NodesController.class);
	private static final Configuration conf = SettingsServlet.getConfig();
	private @Context SecurityContext security; 

	@GET
	@RolesAllowed({"user", "rwshareuser", "roshareuser"})
	public Response getNodeXml(@QueryParam("uri") String uri, @DefaultValue("max") @QueryParam("detail") String detail, @DefaultValue("xml") @QueryParam("view") String view) {
		return getNodeXml("", uri, detail, view);
	}
	
	/**
	 * Method to return the node description in VOSpace XML format
	 * @return The node description in VOSpace XML format
	 */
	@GET @Path("{path:.+}")
	@RolesAllowed({"user", "rwshareuser", "roshareuser"})
	public Response getNodeXml(@PathParam("path") String fullPath, @QueryParam("uri") String uri, @DefaultValue("max") @QueryParam("detail") String detail, @DefaultValue("xml") @QueryParam("view") String view) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}
		logger.debug("Get node: "+identifier);

		MediaType type = MediaType.TEXT_XML_TYPE;
		if(view.equals("json"))
			type = MediaType.APPLICATION_JSON_TYPE;
		else if(view.equals("xml"))
			type = MediaType.TEXT_XML_TYPE;
		
		Node node = NodeFactory.getNode(identifier, user.getName());
		
		if(view != null && view.equals("data")) {// return the data contents
			return Response.ok(node.exportData()).
					header("Content-Disposition", "attachment; filename="+identifier.getNodePath().getNodeName()).
					header("Content-Length", Long.toString(node.getNodeInfo().getSize())).
					build();
		} else {
			byte[] nodeBytes;
			switch(node.getType()) {
				case DATA_NODE:
					nodeBytes = (byte[])((DataNode)node).export(view, Node.Detail.valueOf(Node.Detail.class, detail));
					break;
				case CONTAINER_NODE:
					nodeBytes = (byte[])((ContainerNode)node).export(view, Node.Detail.valueOf(Node.Detail.class, detail));
					break;
				case STRUCTURED_DATA_NODE:
					nodeBytes = (byte[])((StructuredDataNode)node).export(view, Node.Detail.valueOf(Node.Detail.class, detail));
					break;
				case UNSTRUCTURED_DATA_NODE:
					nodeBytes = (byte[])((UnstructuredDataNode)node).export(view, Node.Detail.valueOf(Node.Detail.class, detail));
					break;
				default:
					nodeBytes = (byte[])node.export(view, Node.Detail.valueOf(Node.Detail.class, detail));
			}

			InputStream xmlStream = new ByteArrayInputStream(nodeBytes);

		    ByteArrayOutputStream out = new ByteArrayOutputStream();
		    Serializer serializer = new Serializer(out);
		    serializer.setIndent(4);  // or whatever you like
		    try {
				serializer.write(new Builder().build(xmlStream));
			} catch (Exception e) {
				logger.error("Error parsing the output node XML document: "+e.getMessage());
				throw new InternalServerErrorException("InternalFault");
			}
			return Response.ok(out.toByteArray()).type(type).build();
		}
	}
	
	/**
	 * Create new node
	 * @param fullPath the path to the new node
	 * @param headers HTTP headers of the request
	 * @param node XML node template
	 * @return The XML node description
	 */
	@PUT @Path("{path:.+}")
	@RolesAllowed({"user", "rwshareuser"})
	public Response createNode(@PathParam("path") String fullPath, byte[] nodeBytes) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}
		
		Node newNode = NodeFactory.createNode(identifier, user.getName(), getType(nodeBytes));
		if (!newNode.hasValidParent()) throw new InternalServerErrorException("ContainerNotFound");
		
		// Does node already exist?
		if(newNode.isStoredMetadata()) {
			throw new ConflictException("A Node already exists with the requested URI."); 
		}

		newNode.setNode(new String(nodeBytes));
		logger.debug("Created node "+fullPath);
		return Response.ok(newNode.export("", Detail.properties)).status(201).build();
    }

	private static NodeType getType(byte[] bytes) {
		String doc = new String(bytes).replace("'", "\"");
		int start = doc.indexOf("\"", doc.indexOf("xsi:type"));
		int end = doc.indexOf("\"", start + 1);
		String type = doc.substring(start + 1, end);
		return NodeType.fromString(type.substring(type.indexOf(":") + 1));
	}
	
	/**
	 * Delete a node
	 * @param fullPath The path of the node
	 * @param headers request headers
	 * @return HTTP result
	 */
	@DELETE @Path("{path:.+}")
	@RolesAllowed({"user", "rwshareuser"})
    public Response deleteNode(@PathParam("path") String fullPath) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		
		try {
			String username = user.getName();
			VospaceId identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
			Node node = NodeFactory.getNode(identifier, username);
			node.markRemoved();
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}
		return Response.ok().build();
    }

	/**
	 * setNode implementation
	 * @param fullPath the node path
	 * @param headers
	 * @param node Node representation to merge with
	 * @return The new node representation
	 */
	@POST @Path("{path:.+}")
	@Produces(MediaType.TEXT_XML)
	@RolesAllowed({"user", "rwshareuser"})
    public Response setNode(@PathParam("path") String fullPath, @Context HttpHeaders headers, byte[] nodeBytes) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		Node currentNode = NodeFactory.getNode(identifier, user.getName());

		if(!currentNode.isStoredMetadata())
			throw new NotFoundException("NodeNotFound");
			
		currentNode.setNode(new String(nodeBytes));
		
		
		return Response.ok(currentNode.export("", Detail.properties)).build();
    }

    
}
