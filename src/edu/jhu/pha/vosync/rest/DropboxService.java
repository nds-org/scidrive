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
package edu.jhu.pha.vosync.rest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URISyntaxException;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.TokenBuffer;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;
import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.api.AccountInfo;
import edu.jhu.pha.vospace.jobs.JobsProcessor;
import edu.jhu.pha.vospace.meta.MetaStoreDistributed;
import edu.jhu.pha.vospace.meta.MetaStore;
import edu.jhu.pha.vospace.meta.MetaStoreFactory;
import edu.jhu.pha.vospace.meta.RegionsInfo;
import edu.jhu.pha.vospace.node.ContainerNode;
import edu.jhu.pha.vospace.node.DataNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodeInfo;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.node.Node.Detail;
import edu.jhu.pha.vospace.oauth.UserHelper;
import edu.jhu.pha.vospace.oauth.VoboxUser;
import edu.jhu.pha.vospace.rest.JobDescription;
import edu.jhu.pha.vospace.rest.JobDescription.DIRECTION;
import edu.jhu.pha.vosync.exception.BadRequestException;
import edu.jhu.pha.vosync.exception.ForbiddenException;
import edu.jhu.pha.vosync.exception.InternalServerErrorException;
import edu.jhu.pha.vosync.exception.NotFoundException;
import edu.jhu.pha.vosync.meta.Chunk;
import edu.jhu.pha.vosync.meta.VoSyncMetaStore;

/**
 * @author Dmitry Mishin
 */
@Path("/1/")
public class DropboxService {
	
	private static final Logger logger = Logger.getLogger(DropboxService.class);
	private @Context SecurityContext security; 
	private static final Configuration conf = SettingsServlet.getConfig();

	private static final JsonFactory f = new JsonFactory();
	
	private static final double GIGABYTE = 1024.0*1024.0*1024.0;

	
	@Path("fileops/copy")
	@POST
	@RolesAllowed({"user", "rwshareuser"})
	public Response copy(@FormParam("root") String root, @FormParam("from_path") String fromPath, @FormParam("to_path") String toPath) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		
		if(null == root || root.isEmpty())
			throw new BadRequestException("Not found parameter root");
		
		if(null == fromPath || fromPath.isEmpty())
			throw new BadRequestException("Not found parameter fromPath");
		
		if(null == toPath || toPath.isEmpty())
			throw new BadRequestException("Not found parameter toPath");
		
		VospaceId fromId, toId;
		try {
			fromId = new VospaceId(new NodePath(fromPath, user.getRootContainer()));
			toId = new VospaceId(new NodePath(toPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		Node node;
		try {
			node = NodeFactory.getNode(fromId, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(fromId.getNodePath().getNodeStoragePath());
		}
		node.copy(toId);
		
		return Response.ok(NodeFactory.getNode(toId, user.getName()).export("json-dropbox",Detail.min)).build();
	}

	@Path("fileops/create_folder")
	@POST
	@RolesAllowed({"user", "rwshareuser"})
	public Response createFolder(@FormParam("root") String root, @FormParam("path") String path) {
		logger.debug("Creating folder "+path);
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		
		if(null == root || root.isEmpty())
			throw new BadRequestException("Not found parameter root");
		
		if(null == path)
			throw new BadRequestException("Not found parameter path");
		
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(path, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		Node node = NodeFactory.createNode(identifier, user.getName(), NodeType.CONTAINER_NODE);
		
		node.createParent();
		node.setNode(null);

		return Response.ok(node.export("json-dropbox",Detail.min)).build();
	}

	@Path("fileops/delete")
	@POST
	@RolesAllowed({"user", "rwshareuser"})
	public Response delete(@FormParam("root") String root, @FormParam("path") String path) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		if(null == root || root.isEmpty())
			throw new BadRequestException("Not found parameter root");
		
		if(null == path || path.isEmpty())
			throw new BadRequestException("Not found parameter path");
		
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(path, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		Node node;
		try {
			node = NodeFactory.getNode(identifier, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(identifier.getNodePath().getNodeStoragePath());
		}
		node.markRemoved();
		
		return Response.ok(NodeFactory.getNode(identifier, user.getName()).export("json-dropbox",Detail.min)).build();
	}

	@GET @Path("account/info")
	@RolesAllowed({"user", "rwshareuser", "roshareuser"})
	public Response getAccountInfo() {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		try {
	    	ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			JsonGenerator g2 = f.createJsonGenerator(byteOut).useDefaultPrettyPrinter();
	
			AccountInfo info = UserHelper.getAccountInfo(user.getName());
			
			g2.writeStartObject();
			
			g2.writeStringField("referral_link", "");
			g2.writeStringField("display_name", info.getUsername());
			g2.writeStringField("uid", "0");
			g2.writeStringField("country", "US");
			
			g2.writeFieldName("quota_info");
			g2.writeStartObject();
			
			g2.writeNumberField("shared",0);
			g2.writeNumberField("quota",info.getSoftLimit());
			g2.writeNumberField("normal",info.getBytesUsed()/GIGABYTE);
			
			g2.writeEndObject();
			
			g2.close();
			byteOut.close();
	
			return Response.ok(byteOut.toByteArray()).build();
		} catch(IOException ex) {
			throw new InternalServerErrorException(ex.getMessage());
		}
	}
	
	@PUT @Path("account/service")
	@RolesAllowed({"user"})
	public Response setAccountService(InputStream serviceCredInpStream) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode credNode = mapper.readTree(serviceCredInpStream);
			UserHelper.updateUserService(user.getName(), credNode);
			return Response.ok().build();
		} catch(IOException ex) {
			throw new InternalServerErrorException(ex.getMessage());
		}
	}
	
	@GET @Path("account/service")
	@RolesAllowed({"user"})
	public Response getAccountService() {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		return Response.ok(UserHelper.getUserServices(user.getName()).toString()).build();
	}
	
	@GET @Path("regions/info")
	@RolesAllowed({"user"})
	public Response getRegionsInfo() {
		MetaStore mstore = MetaStoreFactory.getInstance().getMetaStore(null);
		if(mstore instanceof MetaStoreDistributed) {
			RegionsInfo regionsInfo = ((MetaStoreDistributed)mstore).getRegionsInfo();
			return Response.ok(regionsInfo.toJson()).build();
		}
		throw new InternalServerErrorException("Unsupported");
	}
	
	@GET @Path("files/{root:dropbox|sandbox}/{path:.+}")
	@RolesAllowed({"user", "rwshareuser", "roshareuser"})
	public Response getFile(@PathParam("root") String root, @PathParam("path") String fullPath) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		Node node;
		try {
			node = NodeFactory.getNode(identifier, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(identifier.getNodePath().getNodeStoragePath());
		}

		InputStream nodeInputStream;
		try {
			nodeInputStream = node.exportData(); 
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			logger.error("Node "+node.getUri().toString()+" data  not found.");
			throw new NotFoundException(identifier.getId().toASCIIString());
		}
		
		logger.debug("Node "+node.getUri().toString()+" size: "+node.getNodeInfo().getSize());
		
		ResponseBuilder response = Response.ok(nodeInputStream);
		response.header("x-dropbox-metadata", new String((byte[])(node.export("json-dropbox", Detail.min))));
		response.header("Content-Disposition", "attachment; filename="+identifier.getNodePath().getNodeName());
		response.header("Content-Length", Long.toString(node.getNodeInfo().getSize()));
		
		return response.build();
	}

	@GET @Path("regions/{path:.+}")
	@RolesAllowed({"user"})
	public Response getFileRegions(@PathParam("path") String fullPath) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}
		Node node;
		try {
			node = NodeFactory.getNode(identifier, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(identifier.getNodePath().getNodeStoragePath());
		}

		if(node.getType() == NodeType.CONTAINER_NODE) {
			List<String> nodeRegions = ((ContainerNode)node).getNodeRegions();
			StringBuilder builder = new StringBuilder();
			builder.append("[");
			boolean first = true;
			for(String region: nodeRegions) {
				if(!first)
					builder.append(",");
				else
					first = false;
				builder.append("{\"id\": \""+region+"\"}");
			}
			
			builder.append("]");
			return Response.ok(builder.toString()).build();
		} else {
			throw new BadRequestException("ContainerNotFound");
		}
	}
	
	@GET @Path("metadata/{root:dropbox|sandbox}/{path:.+}")
	@RolesAllowed({"user", "rwshareuser", "roshareuser"})
	public Response getFileMetadata(@PathParam("root") String root, @PathParam("path") String fullPath, @QueryParam("list") @DefaultValue("true") Boolean list, @QueryParam("file_limit") @DefaultValue("25000") int file_limit,  @QueryParam("start") @DefaultValue("0") int start, @QueryParam("count") @DefaultValue("-1") int count) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			logger.debug(e.getMessage());
			throw new BadRequestException("InvalidURI");
		}

		Node node;
		try {
			node = NodeFactory.getNode(identifier, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(identifier.getNodePath().getNodeStoragePath());
		}
		
		Detail detailLevel = list?Detail.max:Detail.min;
		
		long time = System.currentTimeMillis();
		byte[] nodeExport;
		try {
			if(node.getType() == NodeType.CONTAINER_NODE) {
				nodeExport = (byte[])(((ContainerNode)node).export("json-dropbox", detailLevel, start, count));
			} else {
				nodeExport = (byte[])(node.export("json-dropbox", detailLevel));
			}
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(identifier.getId().toASCIIString());
		}
		logger.debug("Generated node contents in "+(System.currentTimeMillis()-time)/1000.0);
		
		return Response.ok(nodeExport).build();
	}

	@GET @Path("metadata/{root:dropbox|sandbox}")
	@RolesAllowed({"user", "shareuser", "readonlyshareuser"})
	public Response getRootMetadata(@PathParam("root") String root, @QueryParam("list") @DefaultValue("true") Boolean list) {
		return getFileMetadata(root, "", list, 25000, 0, -1);
	}
	
	@GET @Path("transfers/info")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({"user"})
	public byte[] getTransfersInfo() {
		final VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		return DbPoolServlet.goSql("Get transfers queue",
	    		"select id, state, direction, starttime, endtime, target from jobs JOIN user_identities ON jobs.user_id = user_identities.user_id WHERE identity = ?",
	            new SqlWorker<byte[]>() {
	                @Override
	                public byte[] go(Connection conn, PreparedStatement stmt) throws SQLException {
	                	
	                	stmt.setString(1, user.getName());
	                	
	                	ByteArrayOutputStream byteOut = null;
	                	try {
		                	JsonFactory f = new JsonFactory();
					    	byteOut = new ByteArrayOutputStream();
							JsonGenerator g2 = f.createJsonGenerator(byteOut);
	
	        				g2.writeStartArray();

	        				ResultSet resSet = stmt.executeQuery();
		        			while(resSet.next()) {
		        				
		        				g2.writeStartObject();
		        				g2.writeStringField("id", resSet.getString("id"));
		        				g2.writeStringField("state", resSet.getString("state"));
		        				g2.writeStringField("direction", resSet.getString("direction"));
		        				g2.writeStringField("starttime", (null != resSet.getTimestamp("starttime")?resSet.getTimestamp("starttime").toString():""));
		        				g2.writeStringField("endtime", (null != resSet.getTimestamp("endtime")?resSet.getTimestamp("starttime").toString():""));
		        				g2.writeStringField("path", resSet.getString("target"));
		        				g2.writeEndObject();
		        	    	}

		        			g2.writeEndArray();
		        			g2.close();
            				byteOut.close();
	        				
		        			return byteOut.toByteArray();
	            		} catch(IOException ex) {
	            			throw new InternalServerErrorException(ex);
	            		}
	                }
	            }
	    );
	}
	
	@Path("fileops/move")
	@POST
	@RolesAllowed({"user", "rwshareuser"})
	public Response move(@FormParam("root") String root, @FormParam("from_path") String fromPath, @FormParam("to_path") String toPath) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		
		if(!user.isWriteEnabled()) {
			throw new ForbiddenException("ReadOnly");
		}
		
		if(null == root || root.isEmpty())
			throw new BadRequestException("Not found parameter root");
		
		if(null == fromPath || fromPath.isEmpty())
			throw new BadRequestException("Not found parameter fromPath");
		
		if(null == toPath || toPath.isEmpty())
			throw new BadRequestException("Not found parameter toPath");
		
		VospaceId fromId, toId;
		try {
			fromId = new VospaceId(new NodePath(fromPath, user.getRootContainer()));
			toId = new VospaceId(new NodePath(toPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		Node node;
		try {
			node = NodeFactory.getNode(fromId, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(fromId.getNodePath().getNodeStoragePath());
		}
		node.move(toId);
		
		return Response.ok(NodeFactory.getNode(toId, user.getName()).export("json-dropbox",Detail.min)).build();
	}

	@POST @Path("files/{root:dropbox|sandbox}/{path:.+}")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	/*TODO Test the method */
	@RolesAllowed({"user", "rwshareuser"})
	public Response postFile(@PathParam("root") String root, @PathParam("path") String fullPath, @FormDataParam("file") InputStream fileDataInp, @FormDataParam("file") FormDataContentDisposition fileDetail, @QueryParam("overwrite") @DefaultValue("true") Boolean overwrite) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		if(!user.isWriteEnabled()) {
			throw new ForbiddenException("ReadOnly");
		}
		
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		MetaStore metastore = MetaStoreFactory.getInstance().getMetaStore(user.getName());
		
		Node node;
		if(identifier.getNodePath().getParentPath().isRoot(false)){
			throw new NotFoundException("Is a root folder");
		} else {
			if(!overwrite) {
				if(metastore.isStored(identifier)){
					logger.debug("Found conflict in node "+identifier);
					String currentFile = identifier.toString();
					String fileName = currentFile.substring(currentFile.lastIndexOf("/")+1);
					if(fileName.contains("."))
						fileName = fileName.substring(0,fileName.lastIndexOf('.'));
					
					int current_num = 1;
					try {
						VospaceId newId = new VospaceId(currentFile.replaceAll(fileName, fileName+"_"+current_num++));
						while(metastore.isStored(newId)){
							logger.debug("Node "+newId.toString()+" exists.");
							newId = new VospaceId(currentFile.replaceAll(fileName, fileName+"_"+current_num++));
						}
						logger.debug("Node "+newId.toString()+" not exists.");
						node = (DataNode)NodeFactory.createNode(newId, user.getName(), NodeType.DATA_NODE);
						node.createParent();
						node.setNode(null);
					} catch(URISyntaxException e) {
						throw new InternalServerErrorException("InvalidURI");
					}
				} else {
					node = (DataNode)NodeFactory.createNode(identifier, user.getName(), NodeType.DATA_NODE);
					node.createParent();
					node.setNode(null);
				}
			} else {
				try {
					node = NodeFactory.getNode(identifier, user.getName());
				} catch(NotFoundException ex) {
					node = (DataNode)NodeFactory.createNode(identifier, user.getName(), NodeType.DATA_NODE);
					node.createParent();
					node.setNode(null);
				}
			}
		}
		
		if(!(node instanceof DataNode)) {
			throw new NotFoundException("Is a container");
		}
		
		((DataNode)node).setData(fileDataInp);
		
		return Response.ok(node.export("json-dropbox",Detail.max)).build();
	}

	@PUT @Path("files_put/{root:dropbox|sandbox}/{path:.+}")
	@RolesAllowed({"user", "rwshareuser"})
	public Response putFile(@PathParam("root") String root, @PathParam("path") String fullPath, InputStream fileDataInp, @QueryParam("overwrite") @DefaultValue("true") Boolean overwrite, @QueryParam("parent_rev") String parentRev) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		MetaStore metastore = MetaStoreFactory.getInstance().getMetaStore(user.getName());
		
		Node node;
		if(identifier.getNodePath().getParentPath().isRoot(false)){
			throw new NotFoundException("Is a root folder");
		} else {
			if(parentRev != null) {
				if(metastore.isStored(identifier)){
					//throw new BadRequestException(identifier.toString()+" exists.");
					//logger.debug("Found conflict in node "+identifier);
					node = NodeFactory.getNode(identifier, user.getName());
					
					if(!parentRev.equals(Integer.toString(node.getNodeInfo().getRevision()))) {
						throw new BadRequestException("Revision mismatch: current is "+node.getNodeInfo().getRevision());

						//TODO fix the revisions
						/*logger.debug("Revisions do not match: "+parentRev+" "+node.getNodeInfo().getCurrentRev());
					
						String currentFile = identifier.toString();
						String fileName = currentFile.substring(currentFile.lastIndexOf("/")+1);
						if(fileName.contains("."))
							fileName = fileName.substring(0,fileName.lastIndexOf('.'));
						
						int current_num = 1;
						try {
							VospaceId newId = new VospaceId(currentFile.replaceAll(fileName, fileName+"_"+current_num++));
							while(metastore.isStored(newId)){
								newId = new VospaceId(currentFile.replaceAll(fileName, fileName+"_"+current_num++));
							}
							node = (DataNode)NodeFactory.getDefaultNode(newId, username);
							node.createParent();
							node.setNode();
						} catch(URISyntaxException e) {
							throw new InternalServerErrorException("InvalidURI");
						}*/
					}
				} else {
					node = (DataNode)NodeFactory.createNode(identifier, user.getName(), NodeType.DATA_NODE);
					node.createParent();
					node.setNode(null);
				}
			} else {
				try {
					node = NodeFactory.getNode(identifier, user.getName());
				} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
					node = (DataNode)NodeFactory.createNode(identifier, user.getName(), NodeType.DATA_NODE);
					node.createParent();
					node.setNode(null);
				}
			}
		}
		
		if(!(node instanceof DataNode)) {
			throw new NotFoundException("Node is a container");
		}
		
		((DataNode)node).setData(fileDataInp);
		Response resp =Response.ok(node.export("json-dropbox",Detail.max)).build(); 
		return resp;
	}
		
	@Path("search/{root:dropbox|sandbox}/{path:.+}")
	@GET
	@RolesAllowed({"user", "rwshareuser", "roshareuser"})
	public byte[] search(@PathParam("root") String root, @PathParam("path") String fullPath, @QueryParam("query") String query, @QueryParam("file_limit") @DefaultValue("1000") int fileLimit, @QueryParam("include_deleted") @DefaultValue("false") boolean includeDeleted) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());


		if(null == query || query.length() < 3){
			throw new BadRequestException("Wrong query parameter");
		}
		
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		Node node;
		try {
			node = NodeFactory.getNode(identifier, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(identifier.getNodePath().getNodeStoragePath());
		}
		
		if(!(node instanceof ContainerNode)) {
			throw new NotFoundException("Not a container");
		}
		
		List<VospaceId> nodesList = ((ContainerNode)node).search(query, fileLimit, includeDeleted);

    	TokenBuffer g = new TokenBuffer(null);

		try {
			g.writeStartArray();

			int ind = 0;
			for(VospaceId childNodeId: nodesList) {
				Node childNode = NodeFactory.getNode(childNodeId, user.getName());
				JsonNode jnode = (JsonNode)childNode.export("json-dropbox-object", Detail.min); 
				g.writeTree(jnode);

				/*CharBuffer cBuffer = Charset.forName("ISO-8859-1").decode(ByteBuffer.wrap((byte[])childNode.export("json-dropbox", Detail.min)));
				while(cBuffer.remaining() > 0)
					g.writeRaw(cBuffer.get());
				if(ind++ < nodesList.size()-1)
					g.writeRaw(',');*/
			}
			
			g.writeEndArray();

	    	ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
	    	MappingJsonFactory f = new MappingJsonFactory();
			JsonGenerator g2 = f.createJsonGenerator(byteOut).useDefaultPrettyPrinter();
			g.serialize(g2);
			g2.close();
			byteOut.close();

			return byteOut.toByteArray();
		} catch (JsonGenerationException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error generationg JSON: "+e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error generationg JSON: "+e.getMessage());
		} finally {
			try { g.close(); } catch(IOException ex) {}
		}
		
	}

	@Path("cont_search")
	@GET
	@RolesAllowed({"user", "rwshareuser"})
	public Response contSearch(@QueryParam("query") String queryStr) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		try {
			Directory directory = FSDirectory.open(new File(conf.getString("lucene.index")));
			
		    DirectoryReader ireader = DirectoryReader.open(directory);
		    IndexSearcher isearcher = new IndexSearcher(ireader);
		    
		    Analyzer analyzer = new EnglishAnalyzer(Version.LUCENE_41);
		    QueryParser parser = new QueryParser(Version.LUCENE_41, "content", analyzer);
		    String queryFullStr = "owner:\""+user.getName()+"\" AND "+queryStr;
		    Query query = parser.parse(queryFullStr);
		    ScoreDoc[] hits = isearcher.search(query, null, 100).scoreDocs;
		    
		    StringBuffer buf = new StringBuffer();
		    
		    buf.append("<p>Results: "+hits.length+"</p>");
		    
		    for (int i = 0; i < hits.length; i++) {
		      Document hitDoc = isearcher.doc(hits[i].doc);
		      buf.append("<h3>"+hitDoc.get("source")+"</h3>");
		      if(hitDoc.get("content").length() > 1024)
		    	  buf.append("<p>"+hitDoc.get("content").substring(0,1024)+"...</p>");
		      else
		    	  buf.append("<p>"+hitDoc.get("content")+"</p>");
		    }
		    analyzer.close();
		    ireader.close();
		    directory.close();

		    return Response.ok(buf.toString()).build();
		} catch(Exception ex) {
			ex.printStackTrace();
			return Response.ok().build();
		}
	}
	
	@PUT @Path("regions_put/{path:.+}")
	@RolesAllowed({"user"})
	public Response putFileRegions(@PathParam("path") String fullPath, InputStream regionsInpStream) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		VospaceId identifier;
		
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}
		Node node;
		try {
			node = NodeFactory.getNode(identifier, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(identifier.getNodePath().getNodeStoragePath());
		}

		if(node.getType() == NodeType.CONTAINER_NODE) {
			try {
				Map<String, String> regions = new HashMap<String, String>();
				
				BufferedReader bufferedRead = new BufferedReader(new InputStreamReader(regionsInpStream));
				
				String curRegion = bufferedRead.readLine();
				String firstRegion = new String(curRegion);
				if(null == curRegion)
					throw new BadRequestException("No regions defined");
				
				String syncRegion = bufferedRead.readLine();
				do {
					regions.put(curRegion, syncRegion);
					if(null != syncRegion) {
						curRegion = new String(syncRegion);
						syncRegion = bufferedRead.readLine();
					}
				} while(null != syncRegion);
				
				if(null != regions.get(firstRegion)) {
					regions.put(curRegion, firstRegion);
				}
				
				bufferedRead.close();
				
				((ContainerNode)node).setNodeRegions(regions);
				
				return Response.ok().build();
			} catch(IOException ex) {
				throw new InternalServerErrorException("Error reading PUT request");
			}
		} else {
			throw new BadRequestException("ContainerNotFound");
		}
	}

	/**
	 * Create new share
	 * @param root
	 * @param fullPath
	 * @param group
	 * @param write_perm
	 * @return
	 */
	@PUT @Path("shares/{root:dropbox|sandbox}/{path:.+}")
	@RolesAllowed({"user"})
	public byte[] putShares(@PathParam("root") String root, @PathParam("path") String fullPath, @QueryParam("group") String group, @DefaultValue("false") @QueryParam("write_perm") Boolean write_perm) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			logger.debug(e.getMessage());
			throw new BadRequestException("InvalidURI");
		}

		Node node;
		try {
			node = NodeFactory.getNode(identifier, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(identifier.getNodePath().getNodeStoragePath());
		}

		
    	ByteArrayOutputStream byteOut = null;
    	try {
        	JsonFactory f = new JsonFactory();
	    	byteOut = new ByteArrayOutputStream();
			JsonGenerator g2 = f.createJsonGenerator(byteOut);

			g2.writeStartObject();
			g2.writeStringField("id", node.getMetastore().createShare(node.getUri(), group, write_perm));
			g2.writeStringField("uri", "");
			g2.writeStringField("expires", "never");
			g2.writeEndObject();

			g2.close();
			byteOut.close();
			
			return byteOut.toByteArray();
		} catch(IOException ex) {
			throw new InternalServerErrorException(ex);
		}
	}

	@GET @Path("shares")
	@RolesAllowed({"user"})
	public byte[] getShares() {
		final VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		
    	ByteArrayOutputStream byteOut = null;
    	try {
        	JsonFactory f = new JsonFactory();
	    	byteOut = new ByteArrayOutputStream();
			final JsonGenerator g2 = f.createJsonGenerator(byteOut);

			g2.writeStartArray();

			DbPoolServlet.goSql("Get shares",
	        		"select share_id, container_name, group_name, share_write_permission FROM container_shares "+
	        		"LEFT JOIN groups ON container_shares.group_id = groups.group_id "+
	        		"JOIN containers ON container_shares.container_id = containers.container_id "+
	        		"JOIN user_identities ON containers.user_id = user_identities.user_id WHERE identity = ?",
	                new SqlWorker<Boolean>() {
	                    @Override
	                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
	                    	stmt.setString(1, user.getName());
	            			ResultSet rs = stmt.executeQuery();
	            			while(rs.next()){
	            				try {
	            					g2.writeStartObject();
	            					g2.writeStringField("share_id", rs.getString("share_id"));
	            					g2.writeStringField("container", rs.getString("container_name"));
	            					g2.writeStringField("group", rs.getString("group_name"));
	            					g2.writeBooleanField("write_permission", rs.getBoolean("share_write_permission"));
	            					g2.writeEndObject();
	            				} catch(IOException ex) {logger.error(ex.getMessage());}
	            			}
	            			return true;
	                    }
	                }
	        );
			g2.writeEndArray();

			g2.close();
			byteOut.close();
			return byteOut.toByteArray();
		} catch(IOException ex) {
			throw new InternalServerErrorException(ex);
		}
	}

	@DELETE @Path("shares/{share_id:.+}")
	@RolesAllowed({"user", "rwshareuser"})
	public Response deleteShare(@PathParam("share_id") final String share_id) {
		final VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		DbPoolServlet.goSql("Remove share",
        		"delete container_shares from container_shares "+
        		"JOIN containers ON container_shares.container_id = containers.container_id "+
        		"JOIN user_identities ON containers.user_id = user_identities.user_id WHERE share_id = ? AND identity = ?;",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	stmt.setString(1, share_id);
                    	stmt.setString(2, user.getName());
                    	return stmt.execute();
                    }
                });
		return Response.ok().build();
	}
	

	@Path("share_groups")
	@GET
	@RolesAllowed({"user", "rwshareuser", "roshareuser"})
	public byte[] shareGroups() {
		final VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		ByteArrayOutputStream byteOut = null;
    	try {
        	JsonFactory f = new JsonFactory();
	    	byteOut = new ByteArrayOutputStream();
			final JsonGenerator g2 = f.createJsonGenerator(byteOut);

			g2.writeStartArray();

			DbPoolServlet.goSql("Get share groups",
	        		"select group_id, group_name from groups order by group_name;",
	                new SqlWorker<Boolean>() {
	                    @Override
	                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
	            			ResultSet rs = stmt.executeQuery();
	            			while(rs.next()){
	            				try {
	            					g2.writeStartObject();
	            					g2.writeNumberField("id", rs.getInt(1));
	            					g2.writeStringField("name", rs.getString(2));
	            					g2.writeEndObject();
	            				} catch(IOException ex) {logger.error(ex.getMessage());}
	            			}
	            			return true;
	                    }
	                }
	        );
			g2.writeEndArray();

			g2.close();
			byteOut.close();
			return byteOut.toByteArray();
		} catch(IOException ex) {
			throw new InternalServerErrorException(ex);
		}
		
	}
	
	@Path("share_groups/{group_id}")
	@GET
	@RolesAllowed({"user", "rwshareuser", "roshareuser"})
	public byte[] shareGroupMembers(@PathParam("group_id") final int group_id) {
		final VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		ByteArrayOutputStream byteOut = null;
    	try {
        	JsonFactory f = new JsonFactory();
	    	byteOut = new ByteArrayOutputStream();
			final JsonGenerator g2 = f.createJsonGenerator(byteOut);

			g2.writeStartArray();

			DbPoolServlet.goSql("Get share group members",
	        		"select identity from user_identities JOIN user_groups ON user_identities.user_id = user_groups.user_id WHERE group_id = ? order by identity;",
	                new SqlWorker<Boolean>() {
	                    @Override
	                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
	                    	stmt.setInt(1, group_id);
	            			ResultSet rs = stmt.executeQuery();
	            			while(rs.next()){
	            				try {
	            					g2.writeString(rs.getString(1));
	            				} catch(IOException ex) {logger.error(ex.getMessage());}
	            			}
	            			return true;
	                    }
	                }
	        );
			g2.writeEndArray();

			g2.close();
			byteOut.close();
			return byteOut.toByteArray();
		} catch(IOException ex) {
			throw new InternalServerErrorException(ex);
		}
		
	}
	
	@Path("media/{root:dropbox|sandbox}/{path:.+}")
	@GET
	@RolesAllowed({"user", "rwshareuser", "roshareuser"})
	public Response media(@PathParam("root") String root, @PathParam("path") String fullPath) {
		VoboxUser user = ((VoboxUser)security.getUserPrincipal());

		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			logger.debug(e.getMessage());
			throw new BadRequestException("InvalidURI");
		}

		Node node;
		try {
			node = NodeFactory.getNode(identifier, user.getName());
		} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
			throw new NotFoundException(identifier.getNodePath().getNodeStoragePath());
		}
		JobDescription job = new JobDescription();

		try {
			job.setTarget(node.getUri().toString());
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		job.setDirection(DIRECTION.PULLFROMVOSPACE);
		job.setId(UUID.randomUUID().toString());
		job.setStartTime(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime());
		job.setState(JobDescription.STATE.PENDING);
		job.addProtocol("ivo://ivoa.net/vospace/core#httpget", null);
		job.setUsername(user.getName());
		
		
		Method submitJobMethod;
		try {
			submitJobMethod = JobsProcessor.getImplClass().getMethod("submitJob", String.class, JobDescription.class);
			submitJobMethod.invoke(null, user.getName(), job);
			
	    	ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			JsonGenerator g2 = f.createJsonGenerator(byteOut).useDefaultPrettyPrinter();
	
			g2.writeStartObject();
			
			g2.writeStringField("url", conf.getString("application.url")+"/data/"+job.getId());
			g2.writeStringField("expires", "never (yet)");
			
			g2.writeEndObject();
			
			g2.close();
			byteOut.close();
		
			return Response.ok(byteOut.toByteArray()).build();
		} catch (Exception e) {
			throw new InternalServerErrorException(e.getMessage());
		}
	}
	
	@Path("chunked_upload")
	@PUT
	@RolesAllowed({"user", "rwshareuser"})
	public Response chunkedUpload(@QueryParam("upload_id") String uploadId, @QueryParam("offset") long offset, InputStream fileDataInp) {
		final VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		VoSyncMetaStore voMeta = new VoSyncMetaStore(user.getName());

		logger.debug("New chunk: "+uploadId+" "+offset);
		
		if(null == uploadId) {
			uploadId = RandomStringUtils.randomAlphanumeric(15);
		}
		
		Chunk newChunk = voMeta.getLastChunk(uploadId);
		
		if(offset != newChunk.getChunkStart()) { // return error with proper offset
			logger.error("Wrong offset: "+offset+" should be:"+newChunk.getChunkStart());
			ResponseBuilder errorResp = Response.status(400);
			errorResp.entity(genChunkResponse(uploadId, newChunk.getChunkStart()));
			return errorResp.build();
		}

		VospaceId identifier;
		try {
			
			String chunkNumberString = String.format("%07d", newChunk.getChunkNum());
			
			identifier = new VospaceId(new NodePath("/"+conf.getString("chunked_container")+"/"+uploadId+"/"+chunkNumberString));
			DataNode node = (DataNode)NodeFactory.createNode(identifier, user.getName(), NodeType.DATA_NODE);

			node.getStorage().createContainer(new NodePath("/"+conf.getString("chunked_container")));
			node.getStorage().putBytes(identifier.getNodePath(), fileDataInp);
			
			NodeInfo info = new NodeInfo();
			
			node.getStorage().updateNodeInfo(identifier.getNodePath(), info);
			node.setNodeInfo(info);
			
			newChunk.setSize(info.getSize());
			
			voMeta.putNewChunk(newChunk);
			
			byte[] resp = genChunkResponse(uploadId, newChunk.getChunkStart()+newChunk.getSize());
			
			return Response.ok(resp).build();
		} catch (URISyntaxException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e.getMessage());
		}

	}

	@Path("commit_chunked_upload/{root:dropbox|sandbox}/{path:.+}")
	@POST
	@RolesAllowed({"user", "rwshareuser"})
	public Response commitChunkedUpload(@PathParam("root") String root, @PathParam("path") String fullPath, @FormParam("upload_id") String uploadId, @QueryParam("overwrite") @DefaultValue("true") Boolean overwrite, @QueryParam("parent_rev") String parentRev) {
		final VoboxUser user = ((VoboxUser)security.getUserPrincipal());
		VoSyncMetaStore vosyncMeta = new VoSyncMetaStore(user.getName());
		
		if(null == uploadId || !(vosyncMeta.chunkedExists(uploadId))) {
			throw new BadRequestException("Parameter upload_id is missing or invalid");
		}
		
		VospaceId identifier;
		try {
			identifier = new VospaceId(new NodePath(fullPath, user.getRootContainer()));
		} catch (URISyntaxException e) {
			throw new BadRequestException("InvalidURI");
		}

		MetaStore metastore = MetaStoreFactory.getInstance().getMetaStore(user.getName());
		
		Node node;
		if(identifier.getNodePath().getParentPath().isRoot(false)){
			throw new NotFoundException("Is a root folder");
		} else {
			if(parentRev != null) {
				if(metastore.isStored(identifier)){
					node = NodeFactory.getNode(identifier, user.getName());
					
					if(!parentRev.equals(Integer.toString(node.getNodeInfo().getRevision()))) {
						throw new BadRequestException("Revision mismatch: current is "+node.getNodeInfo().getRevision());
						//TODO fix the revisions
					}
				} else {
					node = (DataNode)NodeFactory.createNode(identifier, user.getName(), NodeType.DATA_NODE);
					node.createParent();
					node.setNode(null);
				}
			} else {
				try {
					node = NodeFactory.getNode(identifier, user.getName());
					if(!overwrite) {
						throw new BadRequestException("Node exists");
					}
				} catch(edu.jhu.pha.vospace.api.exceptions.NotFoundException ex) {
					node = (DataNode)NodeFactory.createNode(identifier, user.getName(), NodeType.DATA_NODE);
					node.createParent();
					node.setNode(null);
				}
			}
		}
		
		if(!(node instanceof DataNode)) {
			throw new NotFoundException("Node is a container");
		}
		
		((DataNode)node).setChunkedData(uploadId);
		Response resp =Response.ok(node.export("json-dropbox",Detail.max)).build(); 
		return resp;
	}
	
	private static byte[] genChunkResponse(String uploadId, long offset) {
		try {
	    	ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			JsonGenerator g2 = f.createJsonGenerator(byteOut).useDefaultPrettyPrinter();
	
			g2.writeStartObject();
			
			g2.writeStringField("upload_id", uploadId);
			g2.writeNumberField("offset", offset);//31337
			g2.writeStringField("expires", "Tue, 19 Jul 2014 21:55:38 +0000");//"Tue, 19 Jul 2011 21:55:38 +0000"
			
			g2.writeEndObject();
			
			g2.close();
			byteOut.close();
			return byteOut.toByteArray();
		} catch (JsonGenerationException e) {
			throw new InternalServerErrorException(e.getMessage());
		} catch (IOException e) {
			throw new InternalServerErrorException(e.getMessage());
		}
	}
}

