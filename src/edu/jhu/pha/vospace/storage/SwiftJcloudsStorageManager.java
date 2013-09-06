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
package edu.jhu.pha.vospace.storage;

import static com.google.common.io.Closeables.closeQuietly;

import java.io.Closeable;
import java.util.Set;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.openstack.swift.options.ListContainerOptions;
import org.jclouds.http.options.GetOptions;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.openstack.swift.CommonSwiftAsyncClient;
import org.jclouds.openstack.swift.CommonSwiftClient;
import org.jclouds.openstack.swift.domain.AccountMetadata;
import org.jclouds.openstack.swift.domain.ContainerMetadata;
import org.jclouds.openstack.swift.domain.MutableObjectInfoWithMetadata;
import org.jclouds.openstack.swift.domain.ObjectInfo;
import org.jclouds.openstack.swift.domain.SwiftObject;
import org.jclouds.openstack.swift.domain.internal.MutableObjectInfoWithMetadataImpl;
import org.jclouds.openstack.swift.domain.internal.SwiftObjectImpl;
import org.jclouds.rest.RestContext;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpException;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;

import com.rackspacecloud.client.cloudfiles.FilesAccountInfo;
import com.rackspacecloud.client.cloudfiles.FilesAuthorizationException;
import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesContainerInfo;
import com.rackspacecloud.client.cloudfiles.FilesException;
import com.rackspacecloud.client.cloudfiles.FilesInvalidNameException;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import com.rackspacecloud.client.cloudfiles.FilesObjectMetaData;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.BadRequestException;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.api.exceptions.PermissionDeniedException;
import edu.jhu.pha.vospace.node.NodeInfo;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.oauth.UserHelper;

public class SwiftJcloudsStorageManager implements StorageManager, Closeable {

	private BlobStore storage;
	private RestContext<CommonSwiftClient, CommonSwiftAsyncClient> swift;

	
	private static final Configuration conf = SettingsServlet.getConfig();
	private static final Logger logger = Logger.getLogger(SwiftJcloudsStorageManager.class);
	
	private SwiftJsonCredentials credentials;
	
	public static String generateRandomCredentials(final String username) {
        return DbPoolServlet.goSql("Generate random credentials",
        		"select username, apikey from storage_users_pool where user_id IS NULL limit 1;",
                new SqlWorker<String>() {
                    @Override
                    public String go(Connection conn, PreparedStatement stmt) throws SQLException {
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()) {
            				String user = rs.getString("username");
            				String password = rs.getString("apikey");
            				PreparedStatement prep = conn.prepareStatement("update storage_users_pool SET user_id = (select user_id from user_identities where identity = ?) where username = ?");
            				prep.setString(1, username);
            				prep.setString(2, user);
            				prep.execute();

            				logger.debug(username+" "+user);
            				
            				StringWriter writer = new StringWriter();
            		    	JsonFactory f = new JsonFactory();
            		    	try {
            		        	JsonGenerator g = f.createJsonGenerator(writer);
            		        	 
            		        	g.writeStartObject();
            		        	g.writeStringField("username", user);
            		        	g.writeStringField("apikey", password);
            		        	g.writeEndObject();

            		        	g.close(); // important: will force flushing of output, close underlying output stream

            		        	return writer.getBuffer().toString();
            		    	} catch(JsonGenerationException ex) {
            					throw new InternalServerErrorException("Error generating user storage credentials. "+ex.getMessage());
            		    	} catch (IOException e) {
            					throw new InternalServerErrorException("Error generating user storage credentials. "+e.getMessage());
            				}
                        
                        } else
            				throw new PermissionDeniedException("The user does not exist.");
                    }
                }
        );
		
	}

	private SwiftJcloudsStorageManager() {
	}

	/**
	 * Default constructor
	 */
	public SwiftJcloudsStorageManager(String username) {
		this.credentials = UserHelper.getDataStoreCredentials(username);

		Iterable<Module> modules = ImmutableSet.<Module> of(
				new Log4JLoggingModule());

		String provider = "swift";

		BlobStoreContext context = ContextBuilder.newBuilder(provider)
				.endpoint(conf.getString("storage.url"))
				.credentials(credentials.getUsername(),credentials.getApikey())
				.modules(modules)
				.buildView(BlobStoreContext.class);
		storage = context.getBlobStore();
		swift = context.unwrap();
	}

	/**
	 * 
	 * Copy the bytes from the specified old location to the specified new location
	 * in the current backend storage
	 * @param oldLocationId The old location of the bytes
	 * @param newLocationId The new location of the bytes
	 */
	@Override
	public void copyBytes(NodePath oldNodePath, NodePath newNodePath, boolean keepBytes) {
//		try {
//			MutableObjectInfoWithMetadata meta = swift.getApi().getObjectInfo(oldNodePath.getContainerName(), oldNodePath.getNodeRelativeStoragePath());
//			try {
//				// if != null then exists manifest for chunked upload
//				if(null != meta.getManifestPrefix() && keepBytes){
//					throw new BadRequestException("Copying files with segments is not supported.");
//				}
//
//				getClient().copyObject(oldNodePath.getContainerName(), oldNodePath.getNodeRelativeStoragePath(), newNodePath.getContainerName(), newNodePath.getNodeRelativeStoragePath());
//				if(!keepBytes)
//					getClient().deleteObject(oldNodePath.getContainerName(), oldNodePath.getNodeRelativeStoragePath());
//			} catch (FilesInvalidNameException e) {
//				throw new NotFoundException("Node Not Found");
//			} catch (HttpException e) {
//				throw new BadRequestException(e);
//			} catch (IOException e) {
//				throw new InternalServerErrorException(e.getMessage());
//			}
//		} catch(FilesNotFoundException ex) {
//			// file does not exist: just exit (can be just empty file in metadata)
//		} catch(Exception ex) {
//			throw new InternalServerErrorException(ex.getMessage());
//		}
//		
//		updateCredentials();
	}
	
	/**
	 * Create a container at the specified location in the current backend storage
	 * @param locationId The location of the container
	 */
	@Override
	public void createContainer(NodePath npath) {
		if(!npath.getNodeRelativeStoragePath().isEmpty()) { // creating a node inside a first level container
			/*if(!getClient().containerExists(npath.getContainerName()))
				throw new NotFoundException("Container "+npath.getContainerName()+" not found.");
			
			if(!getClient().listObjects(npath.getContainerName(), npath.getNodeRelativeStoragePath()).isEmpty())
				throw new BadRequestException("Node "+npath.getNodeRelativeStoragePath()+" already exists.");

			logger.debug("Creating full path "+npath.getContainerName()+", "+npath.getNodeRelativeStoragePath());
			getClient().createFullPath(npath.getContainerName(), npath.getNodeRelativeStoragePath());*/
		} else { // creating first level container (bucket)
			if(!npath.getContainerName().isEmpty() /*is empty when creating the root node for new user */ && 
					!swift.getApi().containerExists(npath.getContainerName())){
				logger.debug("Creating container "+npath.getContainerName());
//					getClient().createContainer(npath.getContainerName());
				swift.getApi().createContainer(npath.getContainerName());
			}
		}
//    		updateCredentials();

	}

	/**
	 * Get the bytes from the specified location in the current backend storage
	 * @param locationId The location of the bytes
	 * @return a stream containing the requested bytes
	 */
	@Override
	public InputStream getBytes(NodePath npath) {
		SwiftObject obj = swift.getApi().getObject(npath.getContainerName(), npath.getNodeRelativeStoragePath(), GetOptions.NONE);
		return obj.getPayload().getInput();
	}

	@Override
	public long getBytesUsed() {
		long bytesUsed = swift.getApi().getAccountStatistics().getBytes();
		return bytesUsed;
	}
	
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.storage.StorageManager#getStorageUrl()
	 */
	@Override
	public String getStorageUrl() {
			return conf.getString("storage.url");
	}
	
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.storage.StorageManager#getNodeSyncAddress(java.lang.String)
	 */
	@Override
	public String getNodeSyncAddress(String container) {
		return swift.getApi().getContainerMetadata(container).getMetadata().get("X-Container-Sync-To");
	}

	/**
	 * Put the bytes from the specified input stream at the specified location in 
	 * the current backend storage
	 * @param location The location for the bytes
	 * @param stream The stream containing the bytes
	 * @param size The stream size
	 */
	@Override
	public void putBytes(NodePath npath, InputStream stream) {
		SwiftObject obj = swift.getApi().newSwiftObject();
		obj.getInfo().setName(npath.getNodeRelativeStoragePath());
		obj.setPayload(stream);
		obj.getInfo().setContentType("application/file");
		swift.getApi().putObject(npath.getContainerName(), obj);
	}

	/**
	 * Remove the bytes at the specified location in the current backend storage
	 * @param locationId The location of the bytes
	 */
	@Override
	public void remove(NodePath npath, boolean removeChunks) {
		
		final int PAGE_SIZE = 1000;
		
		if (npath.getNodeRelativeStoragePath().isEmpty()) {
			PageSet<ObjectInfo> contContent = swift.getApi().listObjects(npath.getContainerName(), ListContainerOptions.Builder.maxResults(PAGE_SIZE));
			while(!contContent.isEmpty()) {
				for(ObjectInfo obj: contContent) {
					try { 
						if(removeChunks) removeObjectSegments(npath.getContainerName(), obj.getName());
						swift.getApi().removeObject(npath.getContainerName(), obj.getName()); 
					} catch (Exception e) {}
				}
				contContent = swift.getApi().listObjects(npath.getContainerName(), ListContainerOptions.Builder.maxResults(PAGE_SIZE));
			}
			swift.getApi().deleteContainerIfEmpty(npath.getContainerName());
		} else {
			PageSet<ObjectInfo> contContent = swift.getApi().listObjects(npath.getContainerName(), ListContainerOptions.Builder.underPath(npath.getNodeRelativeStoragePath()+"/").maxResults(PAGE_SIZE));
			while(!contContent.isEmpty()) {
				for(ObjectInfo obj: contContent) {
						if(removeChunks) removeObjectSegments(npath.getContainerName(), obj.getName());
						swift.getApi().removeObject(npath.getContainerName(), obj.getName()); 
				}
				contContent = swift.getApi().listObjects(npath.getContainerName(), ListContainerOptions.Builder.underPath(npath.getNodeRelativeStoragePath()+"/").maxResults(PAGE_SIZE));
			}
			try {
				if(removeChunks) removeObjectSegments(npath.getContainerName(), npath.getNodeRelativeStoragePath());
				swift.getApi().removeObject(npath.getContainerName(), npath.getNodeRelativeStoragePath()); 
			} catch (Exception e) {}
		}
	}

	/**
	 * @param npath
	 * @param obj
	 * @throws IOException
	 * @throws FilesNotFoundException
	 * @throws HttpException
	 * @throws FilesAuthorizationException
	 * @throws FilesInvalidNameException
	 * @throws FilesException
	 */
	void removeObjectSegments(String containerName, String objectName) {
		MutableObjectInfoWithMetadata meta = swift.getApi().getObjectInfo(containerName, objectName);
		// if != null then exists manifest for chunked upload
		//TODO !!!!!!!!!!!!!!!!
//		if(null != meta.getManifestPrefix()){
//			NodePath path = new NodePath(meta.getManifestPrefix());
//			List<FilesObject> segmList = getClient().listObjects(path.getContainerName(), path.getNodeRelativeStoragePath(), '/');
//			for(FilesObject segm: segmList) {
//				getClient().deleteObject(StorageManagerFactory.CHUNKED_CONTAINER, segm.getName());
//				logger.debug("Deleted segm "+segm.getName());
//			}
//		}
	}
	

	@Override
	public void setNodeSyncTo(String container, String syncTo, String syncKey) {
		throw new AssertionError("Unsupported");
//		try {
//			getClient().setSyncTo(container, syncTo, syncKey);
//		} catch (FilesAuthorizationException e) {
//			throw new InternalServerErrorException(e);
//		} catch (FilesException e) {
//			throw new InternalServerErrorException(e);
//		} catch (IOException e) {
//			throw new InternalServerErrorException(e);
//		} catch (HttpException e) {
//			throw new InternalServerErrorException(e);
//		}
	}

	@Override
	public void updateNodeInfo(NodePath npath, NodeInfo nodeInfo) {
		if(npath.isRoot(false)) { // root node
	    	AccountMetadata accountInfo = swift.getApi().getAccountStatistics();
			nodeInfo.setSize(accountInfo.getBytes());
			nodeInfo.setContentType("application/directory");
    	} else if(npath.getNodeStoragePathArray().length == 1) { // container info
			if(!swift.getApi().containerExists(npath.getContainerName()))
				return;
			
			ContainerMetadata contInfo = swift.getApi().getContainerMetadata(npath.getContainerName());
			nodeInfo.setSize(contInfo.getBytes());
			nodeInfo.setContentType("application/directory");
    	} else { // info for a node inside a container
			if(!swift.getApi().objectExists(npath.getContainerName(), npath.getNodeRelativeStoragePath())) {
		    	nodeInfo.setSize(0);
		    	nodeInfo.setContentType("application/file");
		    	logger.debug("Info for non-existent object");
			} else {
				MutableObjectInfoWithMetadata nodeMeta = swift.getApi().getObjectInfo(npath.getContainerName(), npath.getNodeRelativeStoragePath());
		    	nodeInfo.setSize(nodeMeta.getBytes());
		    	nodeInfo.setContentType(nodeMeta.getContentType());
			}
    	}

	}

	/**
	 * Creates the manifest that combines the chunks into single file.
	 * If the file already exists and points to other chunks, these will be removed.
	 */
	@Override
	public void putChunkedBytes(NodePath nodePath, String chunkedId) {
		try {
			String manifest = StorageManagerFactory.CHUNKED_CONTAINER+"/"+chunkedId;
			try {
				removeObjectSegments(nodePath.getContainerName(), nodePath.getNodeRelativeStoragePath());
				swift.getApi().
				swift.getApi().putObjectManifest(nodePath.getContainerName(), nodePath.getNodeRelativeStoragePath(), manifest);
			} catch(FilesNotFoundException ex) {
				getClient().createManifestObject(nodePath.getContainerName(), "application/file", nodePath.getNodeRelativeStoragePath(), manifest, new Hashtable<String, String>());
			}
		} catch (HttpException e) {
			throw new InternalServerErrorException(e);
		} catch (IOException e) {
			throw new InternalServerErrorException(e);
		}
		updateCredentials();
	}

	private void init() {
		Iterable<Module> modules = ImmutableSet.<Module> of(
				new Log4JLoggingModule());

		String provider = "swift";
//      String identity = "dimm:dimm"; // tenantName:userName
//      String password = "crystal"; // demo account uses ADMIN_PASSWORD too

		String identity = "2ZtyHZPBV4:2ZtyHZPBV4"; // tenantName:userName
		String password = "vUBFIM4IhgyKX8p"; // demo account uses ADMIN_PASSWORD too

		BlobStoreContext context = ContextBuilder.newBuilder(provider)
				.endpoint("http://vobox.pha.jhu.edu:8081/auth/v1.0")
				.credentials(identity, password)
				.modules(modules)
				.buildView(BlobStoreContext.class);
		storage = context.getBlobStore();
		swift = context.unwrap();
	}

   private void listContainers() {
	      System.out.println("List Containers");
	      Set<ContainerMetadata> containers = swift.getApi().listContainers();

	      for (ContainerMetadata container: containers) {
//				    	 swift.getApi().listObjects(arg0, arg1);
	         System.out.println("  " + container);
	      }
	   }

   private void listChunks() {
	      System.out.println("List Containers");
	      Set<ContainerMetadata> containers = swift.getApi().listContainers();

	      for (ContainerMetadata container: containers) {
//				    	 swift.getApi().listObjects(arg0, arg1);
	         System.out.println("  " + container);
	      }
	   }

   public void close() {
      closeQuietly(storage.getContext());
   }

	
   public static void main(String[] s) throws IOException, HttpException {
	   SwiftJcloudsStorageManager jCloudsSwift = new SwiftJcloudsStorageManager();

	   try {
		   jCloudsSwift.init();
		   jCloudsSwift.listContainers();
		   jCloudsSwift.close();
	   }
	   catch (Exception e) {
		   e.printStackTrace();
	   }
	   finally {
		   jCloudsSwift.close();
	   }
   }
	
}
