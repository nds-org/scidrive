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
import org.apache.commons.httpclient.URIException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
import edu.jhu.pha.vospace.api.exceptions.PermissionDeniedException;
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

public class UserHelperImpl implements UserHelper {
	private static final Logger logger = Logger.getLogger(UserHelperImpl.class);
	private final static Configuration conf = SettingsServlet.getConfig();
	
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.UserHelper#getDataStoreCredentials(java.lang.String)
	 */
	@Override
	public HashMap<String, String> getDataStoreCredentials(final String username) {
        return DbPoolServlet.goSql("Retrieving credentials for " + username,
                "select storage_credentials from users where identity = ?;",
                new SqlWorker<HashMap<String, String>>() {
                    @Override
                    public HashMap<String, String> go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username);
                        ResultSet rs = stmt.executeQuery();
                        if (!rs.next())
                            throw new PermissionDeniedException("The user "+username+" does not exist.");

                        String credentials = rs.getString("storage_credentials");
                        if(credentials == null || credentials.length() == 0)
                        	return new HashMap<String, String>();
                        
                    	ObjectMapper mapper = new ObjectMapper();
                    	try {
                    		HashMap<String, String> result = mapper.readValue(rs.getBytes("storage_credentials"), new HashMap<String, String>().getClass());
	                        return result;
	                    } catch(IOException ex) {
	                    	throw new InternalServerErrorException("Unable to read user "+username+" storage credentials");
	                    }
                    }
                });
	}

	/* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#getProcessorCredentials(edu.jhu.pha.vospace.oauth.SciDriveUser)
	 */
	@Override
	public JsonNode getProcessorCredentials(final SciDriveUser username) {
        return DbPoolServlet.goSql("Retrieving processor credentials for " + username,
                "select service_credentials from users where identity = ?;",
                new SqlWorker<JsonNode>() {
                    @Override
                    public JsonNode go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username.getName());
                        ResultSet rs = stmt.executeQuery();
                        if (!rs.next())
                            throw new PermissionDeniedException("The user "+username+" does not exist.");
                        
                    	ObjectMapper mapper = new ObjectMapper();
                    	try {
                    		byte[] credBytes = rs.getBytes("service_credentials");
                    		if(null == credBytes || credBytes.length == 0)
                    			credBytes = "{}".getBytes();
                    		JsonNode allCredentials = mapper.readValue(credBytes, JsonNode.class);
	                        return allCredentials;
	                    } catch(IOException ex) {
	                    	throw new InternalServerErrorException("Unable to read user "+username+" service credentials");
	                    }
                    }
                });
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.UserHelper#setDataStoreCredentials(java.lang.String, java.lang.String)
	 */
	@Override
	public int setDataStoreCredentials(final String username, final String credentials) {
        return DbPoolServlet.goSql("Retrieving credentials for " + username,
                "update users set storage_credentials = ? where identity = ?;",
                new SqlWorker<Integer>() {
                    @Override
                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, credentials);
                        stmt.setString(2, username);
                        int result = stmt.executeUpdate();
                        return result;
                    }
                });
	}

	
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.UserHelper#addDefaultUser(edu.jhu.pha.vospace.oauth.SciDriveUser)
	 */
    @Override
	public synchronized boolean addDefaultUser(final SciDriveUser username) {
		if(DbPoolServlet.goSql("Add new user",
         		"INSERT IGNORE INTO users (identity) "+
				"values(?);",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	stmt.setString(1, username.getName());
                        return stmt.executeUpdate() > 0;
                    }
                }
        )) {

			try {
				logger.debug("Creating new root node");
				VospaceId identifier = new VospaceId(new NodePath("/"));
				Node node = NodeFactory.createNode(identifier, username, NodeType.CONTAINER_NODE);
				if(!node.isStoredMetadata()){
					node.setNode(null);
					ContainerNode firstNode = (ContainerNode)NodeFactory.createNode(identifier.appendPath(new NodePath("/first_container")), username, NodeType.CONTAINER_NODE);
					firstNode.setNode(null);
				}
				logger.debug("new toor node was created");
			} catch(URIException ex) {
				logger.error("Error creating root node for user: "+ex.getMessage());
				return false;
			}
			return true;
		}
		return false;
	}

    
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.UserHelper#getAccountInfo(edu.jhu.pha.vospace.oauth.SciDriveUser)
	 */
	@Override
	public AccountInfo getAccountInfo(final SciDriveUser username) {
		AccountInfo info = DbPoolServlet.goSql("Getting user \"" + username.getName() + "\" limits from DB.",
                "select hardlimit, softlimit, bound_identity from users "
				+ "LEFT OUTER JOIN user_bindings ON users.user_id = user_bindings.user_id "
                + "WHERE identity = ?;",
                new SqlWorker<AccountInfo>() {
                    @Override
                    public AccountInfo go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username.getName());
                        ResultSet rs = stmt.executeQuery();
                        AccountInfo info = new AccountInfo();
                        while (rs.next()){
                            info.setUsername(username.getName());
                            info.setHardLimit(rs.getInt("hardlimit"));
                            info.setSoftLimit(rs.getInt("softlimit"));
                            String boundIdentity = rs.getString("bound_identity");
                            if(null != boundIdentity) {
                            	info.getAliases().add(boundIdentity);
                            }
                        }
                    	return info;
                    }
                }
        );

		StorageManager storage = StorageManagerFactory.getStorageManager(username); 
		info.setBytesUsed(storage.getBytesUsed());
		
		return info;
	}
    
    /*
     * (non-Javadoc)
     * @see edu.jhu.pha.vospace.meta.UserHelper#userExists(edu.jhu.pha.vospace.oauth.SciDriveUser)
     */
    @Override
	public boolean userExists(final SciDriveUser username) {
        return DbPoolServlet.goSql("Checking whether user \"" + username + "\" exists in DB.",
                "select count(identity) from users where identity = ?;",
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
    
    /*
     * (non-Javadoc)
     * @see edu.jhu.pha.vospace.meta.UserHelper#updateUserService(edu.jhu.pha.vospace.oauth.SciDriveUser, java.lang.String, org.codehaus.jackson.JsonNode)
     */
    @Override
	public boolean updateUserService(final SciDriveUser username, final String processorId, JsonNode updateNode) {
        byte[] curNode = DbPoolServlet.goSql("Retrieving user's service credentials from db",
                "select service_credentials from users where identity = ?;",
                new SqlWorker<byte[]>() {
                    @Override
                    public byte[] go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username.getName());
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()) {
                        	byte[] b = rs.getBytes(1);
                        	if(null == b || b.length == 0)
                        		return "{}".getBytes();
                            return b;
                        } else
                            throw new IllegalStateException("No result from query.");
                    }
                }
        );
		ObjectMapper mapper = new ObjectMapper();
    	final JsonNode mainNode;
		try {
			mainNode = mapper.readTree(curNode);
		} catch (Exception e) {
            throw new IllegalStateException("Error parsing user's credentials");
		}

		if(null != updateNode) {
			((org.codehaus.jackson.node.ObjectNode)mainNode).put(processorId, updateNode);
		} else {
			((org.codehaus.jackson.node.ObjectNode)mainNode).remove(processorId);
		}
		
        return DbPoolServlet.goSql("Updating user's service credentials from db",
            "update users set service_credentials = ? where identity = ?",
            new SqlWorker<Boolean>() {
                @Override
                public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                    stmt.setString(1, mainNode.toString());
                    stmt.setString(2, username.getName());
                    return stmt.execute();
                }
            }
        );
    
    }

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.UserHelper#getUserServices(edu.jhu.pha.vospace.oauth.SciDriveUser)
	 */
	@Override
	public JsonNode getUserServices(final SciDriveUser username) {
        byte[] curNode = DbPoolServlet.goSql("Retrieving user's service credentials from db",
                "select service_credentials from users where identity = ?;",
                new SqlWorker<byte[]>() {
                    @Override
                    public byte[] go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username.getName());
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()) {
                        	byte[] b = rs.getBytes(1);
                        	if(null == b || b.length == 0)
                                return "{}".getBytes();
                            return b;
                        } else
                            throw new IllegalStateException("No result from query.");
                    }
                }
        );

		try {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readTree(curNode);
		} catch (Exception e) {
            throw new IllegalStateException("Error parsing user's credentials");
		}
	}
    
    
    /*
     * (non-Javadoc)
     * @see edu.jhu.pha.vospace.meta.UserHelper#getGroups(edu.jhu.pha.vospace.oauth.SciDriveUser)
     */
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
    
    /*
     * (non-Javadoc)
     * @see edu.jhu.pha.vospace.meta.UserHelper#getGroupUsers(edu.jhu.pha.vospace.oauth.SciDriveUser, java.lang.String)
     */
    @Override
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
    
    /*
     * (non-Javadoc)
     * @see edu.jhu.pha.vospace.meta.UserHelper#getSharePermission(java.lang.String, java.lang.String)
     */
	@Override
	public Share getSharePermission(final String requestUserId, final String shareId) {
        return DbPoolServlet.goSql("Checking whether user \"" + requestUserId + "\" can access share.",
                "select container_name, group_id, share_write_permission, identity, storage_credentials from container_shares " +
                "JOIN containers ON container_shares.container_id = containers.container_id " +
                "JOIN users ON containers.user_id = users.user_id "+
                "where share_id = ?;",
                new SqlWorker<Share>() {
                    @Override
                    public Share go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, shareId);
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()) {
                        	
                        	String containerName = rs.getString(1);
                        	String groupId = rs.getString(2);
                        	boolean share_write_permission = rs.getBoolean(3);
                        	String ownerUserId = rs.getString(4);
                        	String storageCredentialsString = rs.getString(5);

                        	rs.close();
                        	
                        	Share.SharePermission permission = (share_write_permission)?Share.SharePermission.RW_USER:Share.SharePermission.RO_USER;

                        	HashMap<String, String> credentials;
                            if(storageCredentialsString == null || storageCredentialsString.length() == 0)
                            	credentials = new HashMap<String, String>();
                            
                        	ObjectMapper mapper = new ObjectMapper();
                        	try {
                        		credentials = mapper.readValue(storageCredentialsString, new HashMap<String, String>().getClass());
    	                    } catch(IOException ex) {
    	                    	throw new InternalServerErrorException("Unable to read user "+ownerUserId+" storage credentials");
    	                    }

                        	if(groupId.isEmpty()) {//anonymous share
                        		logger.debug("group is empty - anon share");
                            	return new Share(ownerUserId, containerName, permission, credentials);
                        	}

                            HttpHead method = new HttpHead(conf.getString("keystone.url")+"/v3/groups/"+groupId+"/users/"+requestUserId);
                            method.setHeader("X-Auth-Token",KeystoneAuthenticator.getAdminToken());
                    		try {
                    	        HttpResponse resp = MyHttpConnectionPoolProvider.getHttpClient().execute(method);
                    	        if (resp.getStatusLine().getStatusCode() == 204) { // user is member of group
                    	        	// returning the share with userId of container owner
                                	return new Share(requestUserId, containerName, permission, credentials);
                    	        } else {
                                	return new Share(requestUserId, "", Share.SharePermission.DENIED, null);
                    	        }
                    		} catch (IOException e) {
                    			logger.error("Error checking group users: "+e.getMessage());
                    			e.printStackTrace();
                    			throw new InternalServerErrorException(e);
                    		}
                        } else
                        	rs.close();
                        	return new Share(requestUserId, "", Share.SharePermission.DENIED, null);
                    }
                }
        );
		
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.UserHelper#getUserIdFromBoundId(java.lang.String)
	 */
	@Override
	public String getUserIdFromBoundId(final String boundName) {
        return DbPoolServlet.goSql("Checking bound users table and returning userId",
                "select identity from users JOIN user_bindings ON users.user_id = user_bindings.user_id where bound_identity = ?;",
                new SqlWorker<String>() {
                    @Override
                    public String go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, boundName);
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next())
                            return rs.getString(1);
                        else {
                        	logger.error("Can't find user bind "+boundName);
                            throw new PermissionDeniedException("The user "+boundName+" is not bound.");
                        }
                    }
                }
        );
	}	
	

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.UserHelper#removeUserAlias(java.lang.String, java.lang.String)
	 */
	@Override
	public void removeUserAlias(final String userName, final String alias) {
        DbPoolServlet.goSql("Removing user's alias",
                "delete from user_bindings WHERE bound_identity = ? and user_id = (select user_id from users where identity = ?);",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, alias);
                        stmt.setString(2, userName);
                        return stmt.execute();
                    }
                }
        );
	}
	
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.UserHelper#addUserAlias(java.lang.String, java.lang.String)
	 */
	@Override
	public void addUserAlias(final String username, final String alias) {
		DbPoolServlet.goSql("Add user alias",
				"insert IGNORE into user_bindings (user_id, bound_identity) " +
				"select users.user_id, ? from users WHERE identity = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	stmt.setString(1, alias);
                    	stmt.setString(2, username);
                        stmt.executeUpdate();
                        return true;
                    }
		});
    }
	

}
