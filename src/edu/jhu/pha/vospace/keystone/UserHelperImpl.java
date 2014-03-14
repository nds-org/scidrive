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
package edu.jhu.pha.vospace.keystone;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.api.AccountInfo;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.jobs.MyHttpConnectionPoolProvider;
import edu.jhu.pha.vospace.meta.Share;
import edu.jhu.pha.vospace.meta.UserGroup;
import edu.jhu.pha.vospace.meta.UserHelper;
import edu.jhu.pha.vospace.node.ContainerNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.oauth.SciDriveUser;
import edu.jhu.pha.vospace.storage.StorageManager;
import edu.jhu.pha.vospace.storage.StorageManagerFactory;

public class UserHelperImpl extends edu.jhu.pha.vospace.oauth.UserHelperImpl implements UserHelper {
	private static final Logger logger = Logger.getLogger(UserHelperImpl.class);
	private final static Configuration conf = SettingsServlet.getConfig();
	
    @Override
	public boolean addDefaultUser(final SciDriveUser username) {
		DbPoolServlet.goSql("Add new user",
         		"insert into users values ()",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.executeUpdate();
                        
                        ResultSet rs = stmt.getGeneratedKeys();

                        if(rs.next()) {
                               final int user_id = rs.getInt(1);
                               
	                       		return DbPoolServlet.goSql("Add user identity",
	                             		"insert into user_identities (user_id, identity) values (?, ?)",
	                                    new SqlWorker<Boolean>() {
	                                        @Override
	                                        public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
	                                        	stmt.setInt(1, user_id);
	                                        	stmt.setString(2, username.getName());
	                                            return stmt.execute();
	                                        }
	                                    }
	                            );
                               
                               
                        }
                        return false;
                    }
                },
                Statement.RETURN_GENERATED_KEYS
        );

		try {
			logger.debug("Creating new toor node");
			VospaceId identifier = new VospaceId(new NodePath("/"));
			Node node = NodeFactory.createNode(identifier, username, NodeType.CONTAINER_NODE);
			if(!node.isStoredMetadata()){
				node.setNode(null);
				ContainerNode firstNode = (ContainerNode)NodeFactory.createNode(identifier.appendPath(new NodePath("/first_container")), username, NodeType.CONTAINER_NODE);
				firstNode.setNode(null);
			}
			logger.debug("new toor node was created");
		} catch(URISyntaxException ex) {
			logger.error("Error creating root node for user: "+ex.getMessage());
			return false;
		}
		
		return true;
	}

    
	@Override
	public AccountInfo getAccountInfo(final SciDriveUser username) {
		AccountInfo info = DbPoolServlet.goSql("Getting user \"" + username + "\" limits from DB.",
                "select hardlimit, softlimit from users JOIN user_identities ON users.user_id = user_identities.user_id WHERE identity = ?;",
                new SqlWorker<AccountInfo>() {
                    @Override
                    public AccountInfo go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username.getName());
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()){
                            AccountInfo info = new AccountInfo();
                            info.setUsername(username.getName());
                            info.setHardLimit(rs.getInt("hardlimit"));
                            info.setSoftLimit(rs.getInt("softlimit"));
                        	return info;
                        } else {
                            throw new IllegalStateException("No result from query.");
                        }
                    }
                }
        );

		StorageManager storage = StorageManagerFactory.getStorageManager(username); 
		info.setBytesUsed(storage.getBytesUsed());
		
		return info;
	}
    
    /** Does the named user exist? */
    @Override
	public boolean userExists(final SciDriveUser username) {
        return DbPoolServlet.goSql("Checking whether user \"" + username + "\" exists in DB.",
                "select count(identity) from user_identities where identity = ?;",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username.getName());
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next())
                            return rs.getInt(1) > 0;
                        else
                            throw new IllegalStateException("No result from query.");
                    }
                }
        );
    }
    
    @Override
	public List<UserGroup> getGroups(final SciDriveUser user) {
    	
    	ArrayList<UserGroup> groups = new ArrayList<UserGroup>();
    	
        HttpGet method = new HttpGet(conf.getString("keystone.url")+"/v3/users/"+user.getName()+"/groups");
        method.setHeader("Content-Type", "application/json");
        method.setHeader("X-Auth-Token",KeystoneAuthenticator.getAdminToken());

		try {
	        HttpResponse resp = MyHttpConnectionPoolProvider.getHttpClient().execute(method);
	        if (resp.getStatusLine().getStatusCode() == 200) {
	            JsonNode groupsNode = new ObjectMapper().readValue(resp.getEntity().getContent(), JsonNode.class);
	            JsonNode groupsList = groupsNode.path("groups");
	            if(null == groupsList || !groupsList.isArray()) {
	            	logger.error("Error reading groups");
	            	throw new InternalServerErrorException("Error reading groups");
	            }
	            for(Iterator<JsonNode> it = groupsList.getElements(); it.hasNext();) {
	            	JsonNode groupNode = it.next();
	            	UserGroup group = new UserGroup();
	            	group.setId(groupNode.get("id").getTextValue());
	            	group.setName(groupNode.get("name").getTextValue());
	            	group.setDescription(groupNode.get("description").getTextValue());
	            	groups.add(group);
	            }
	            return groups;
	        } else {
            	logger.error("Error reading groups");
            	throw new InternalServerErrorException("Error reading groups");
	        }
		} catch (IOException e) {
			logger.error("Error reading groups: "+e.getMessage());
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
    }
    
    public List<String> getGroupUsers(final SciDriveUser user, final String groupId) {
    	ArrayList<String> users = new ArrayList<String>();
    	
        HttpGet method = new HttpGet(conf.getString("keystone.url")+"/v3/groups/"+groupId+"/users");
        method.setHeader("Content-Type", "application/json");
        method.setHeader("X-Auth-Token",KeystoneAuthenticator.getAdminToken());

		try {
	        HttpResponse resp = MyHttpConnectionPoolProvider.getHttpClient().execute(method);
	        if (resp.getStatusLine().getStatusCode() == 200) {
	            JsonNode usersNode = new ObjectMapper().readValue(resp.getEntity().getContent(), JsonNode.class);
	            JsonNode usersList = usersNode.path("users");
	            if(null == usersList || !usersList.isArray()) {
	            	logger.error("Error reading group users");
	            	throw new InternalServerErrorException("Error reading group users");
	            }
	            for(Iterator<JsonNode> it = usersList.getElements(); it.hasNext();) {
	            	JsonNode userNode = it.next();
	            	users.add(userNode.path("name").getTextValue());
	            }
	            return users;
	        } else {
            	logger.error("Error reading group users");
            	throw new InternalServerErrorException("Error reading group users");
	        }
		} catch (IOException e) {
			logger.error("Error reading group users: "+e.getMessage());
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
    }
    

	@Override
	public Share getSharePermission(final String userId, final String shareId) {
        return DbPoolServlet.goSql("Checking whether user \"" + userId + "\" can access share.",
                "select container_name, group_id, share_write_permission, identity from container_shares " +
                "JOIN containers ON container_shares.container_id = containers.container_id " +
                "JOIN user_identities ON containers.user_id = user_identities.user_id where share_id = ?;",
                new SqlWorker<Share>() {
                    @Override
                    public Share go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, shareId);
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()) {
                        	
                        	String containerName = rs.getString(1);
                        	String groupId = rs.getString(2);
                        	boolean share_write_permission = rs.getBoolean(3);
                        	String userId = rs.getString(4);

                        	rs.close();
                        	
                            HttpHead method = new HttpHead(conf.getString("keystone.url")+"/v3//groups/"+groupId+"/users/"+userId);
                            method.setHeader("X-Auth-Token",KeystoneAuthenticator.getAdminToken());
                    		try {
                    	        HttpResponse resp = MyHttpConnectionPoolProvider.getHttpClient().execute(method);
                    	        if (resp.getStatusLine().getStatusCode() == 204) { // user is member of group
                    	        	Share.SharePermission permission = (share_write_permission)?Share.SharePermission.RW_USER:Share.SharePermission.RO_USER;
                    	        	
                    	        	// returning the share with userId of container owner
                                	return new Share(userId, containerName, permission);
                    	        } else {
                                	return new Share(userId, "", Share.SharePermission.DENIED);
                    	        }
                    		} catch (IOException e) {
                    			logger.error("Error checking group users: "+e.getMessage());
                    			e.printStackTrace();
                    			throw new InternalServerErrorException(e);
                    		}
                        } else
                        	rs.close();
                        	return new Share(userId, "", Share.SharePermission.DENIED);
                    }
                }
        );
		
	}	
}
