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
package edu.jhu.pha.vospace.oauth;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.api.AccountInfo;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.PermissionDeniedException;
import edu.jhu.pha.vospace.meta.Share;
import edu.jhu.pha.vospace.meta.UserGroup;
import edu.jhu.pha.vospace.meta.UserHelper;
import edu.jhu.pha.vospace.node.ContainerNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.storage.StorageManager;
import edu.jhu.pha.vospace.storage.StorageManagerFactory;
import edu.jhu.pha.vospace.storage.SwiftJsonCredentials;

public class UserHelperImpl implements UserHelper {
	private static final Logger logger = Logger.getLogger(UserHelperImpl.class);
	
    /* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#setCertificate(edu.jhu.pha.vospace.oauth.SciDriveUser, java.lang.String)
	 */
    @Override
	public void setCertificate(final SciDriveUser username, String certUrl) throws IOException {
        // 1. make sure user exists
        if (!userExists(username))
            throw new IllegalStateException("Unknown user \"" + username + "\".");

        // 2. get certificate from URL
        final byte[] bytes = getBytesFromUrl(certUrl);
        logger.debug("Retrieved input stream from " + certUrl);

        // 3. save to db
        DbPoolServlet.goSql("setting certificate for " + username,
                "update users set certificate = ? where user_id = (select user_id from user_identities where identity = ?)",
                new SqlWorker<Void>() {
                    @Override
                    public Void go(Connection conn, PreparedStatement stmt) throws SQLException {
                        // should we try stmt.setBinaryStream() instead?
                        stmt.setBytes(1, bytes);
                        stmt.setString(2, username.getName());
                        stmt.executeUpdate();
                        logger.debug("Streamed certificate to database.");
                        return null;
                    }
                });
    }

    private static InputStream getInputStreamFromUrl(String urlString) throws IOException {
        URL url = new URL(urlString);
        if (!url.getProtocol().toLowerCase().startsWith("http"))
            throw new UnsupportedOperationException
                    ("URL \"" + urlString + "\" has an unsupported protocol, \"" + url.getProtocol() + "\".");
        else {
            HttpURLConnection urlConn = (HttpURLConnection) new URL(urlString).openConnection();
            urlConn.setConnectTimeout(10000); // millis -- 10 seconds
            urlConn.setRequestMethod("GET");
            urlConn.connect();
            return urlConn.getInputStream();
        }
    }

    private static byte[] getBytesFromUrl(String urlString) throws IOException {
        InputStream in = null;
        try {
            in = getInputStreamFromUrl(urlString);
            return getBytes(in);
        } finally {
        	DbPoolServlet.close(in);
        }
    }

    private static byte[] getBytes(InputStream in) throws IOException {
        List<byte[]> chunks = new ArrayList<byte[]>();
        int sum = 0;
        while(true) {
            byte[] chunk = new byte[1024];
            int n = in.read(chunk);
            // if n == 0, was it a stall?  We'll keep going.
            if (n < 0)
                break;
            else if (n > 0) {
                sum += n;
                // make sure all chunks are all the way full
                if (n < chunk.length)
                    chunk = Arrays.copyOf(chunk, n);
                chunks.add(chunk);
            }
        }
        byte[] result = new byte[sum];
        int n = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, n, chunk.length);
            n += chunk.length;
        }
        return result;
    }

    /* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#getCertificate(java.lang.String)
	 */
    @Override
	public Blob getCertificate(final String username) {
        return DbPoolServlet.goSql("retrieving certificate for " + username,
                "select certificate from users JOIN user_identities ON users.user_id = user_identities.user_id where identity = ?",
                new SqlWorker<Blob>() {
                    @Override
                    public Blob go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username);
                        ResultSet rs = stmt.executeQuery();
                        if (!rs.next())
                            throw new IllegalStateException("Unknown user \"" + username + "\".");
                        return rs.getBlob(1);
                    }
                });
    }

	/* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#getDataStoreCredentials(java.lang.String)
	 */
	@Override
	public HashMap<String, String> getDataStoreCredentials(final String username) {
        return DbPoolServlet.goSql("Retrieving credentials for " + username,
                "select storage_credentials from users JOIN user_identities ON users.user_id = user_identities.user_id where identity = ?;",
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
                "select service_credentials from users JOIN user_identities ON users.user_id = user_identities.user_id where identity = ?;",
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

	/* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#setDataStoreCredentials(java.lang.String, java.lang.String)
	 */
	@Override
	public int setDataStoreCredentials(final String username, final String credentials) {
        return DbPoolServlet.goSql("Retrieving credentials for " + username,
                "update users set storage_credentials = ? where user_id = (select user_id from user_identities where identity = ?);",
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

    /* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#addDefaultUser(edu.jhu.pha.vospace.oauth.SciDriveUser)
	 */
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

		final String storageCredentials = StorageManagerFactory.generateRandomCredentials(username);

		DbPoolServlet.goSql("Update user with credentials",
        		"update users SET storage_credentials = ? where user_id = (select user_id from user_identities where identity = ?)",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
            			stmt.setString(1, storageCredentials);
            			stmt.setString(2, username.getName());
                        return stmt.execute();
                    }
                }
        );

		try {
			VospaceId identifier = new VospaceId(new NodePath("/"));
			Node node = NodeFactory.createNode(identifier, username, NodeType.CONTAINER_NODE);
			if(!node.isStoredMetadata()){
				node.setNode(null);
				ContainerNode firstNode = (ContainerNode)NodeFactory.createNode(identifier.appendPath(new NodePath("/first_container")), username, NodeType.CONTAINER_NODE);
				firstNode.setNode(null);
			}
		} catch(URISyntaxException ex) {
			logger.error("Error creating root node for user: "+ex.getMessage());
			return false;
		}
		
		return true;
	}

    
	/* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#getAccountInfo(edu.jhu.pha.vospace.oauth.SciDriveUser)
	 */
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
    
    /* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#userExists(edu.jhu.pha.vospace.oauth.SciDriveUser)
	 */
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
    
    /* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#updateUserService(edu.jhu.pha.vospace.oauth.SciDriveUser, java.lang.String, org.codehaus.jackson.JsonNode)
	 */
    @Override
	public boolean updateUserService(final SciDriveUser username, final String processorId, JsonNode updateNode) {
        byte[] curNode = DbPoolServlet.goSql("Retrieving user's service credentials from db",
                "select service_credentials from users JOIN user_identities ON users.user_id = user_identities.user_id where identity = ?;",
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
            "update users set service_credentials = ? where user_id = (select user_id from user_identities where identity = ?)",
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

	/* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#getUserServices(edu.jhu.pha.vospace.oauth.SciDriveUser)
	 */
	@Override
	public JsonNode getUserServices(final SciDriveUser username) {
        byte[] curNode = DbPoolServlet.goSql("Retrieving user's service credentials from db",
                "select service_credentials from users JOIN user_identities ON users.user_id = user_identities.user_id where identity = ?;",
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
	
	/* (non-Javadoc)
	 * @see edu.jhu.pha.vospace.oauth.UserHelper#getGroups(edu.jhu.pha.vospace.oauth.SciDriveUser)
	 */
	@Override
	public List<UserGroup> getGroups(final SciDriveUser user) {
    	final ArrayList<UserGroup> groups = new ArrayList<UserGroup>();

		DbPoolServlet.goSql("Get share groups",
        		"select group_id, group_name from groups order by group_name;",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
            			ResultSet rs = stmt.executeQuery();
            			while(rs.next()){
        	            	UserGroup group = new UserGroup();
        	            	group.setId(rs.getString(1));
        	            	group.setName(rs.getString(2));
        	            	group.setDescription(rs.getString(2));
        	            	groups.add(group);
            			}
            			return true;
                    }
                }
        );
		return groups;
	}

	@Override
	public List<String> getGroupUsers(final SciDriveUser user, final String groupId) {
		final ArrayList<String> users = new ArrayList<String>();
		DbPoolServlet.goSql("Get share group members",
	    		"select identity from user_identities JOIN user_groups ON user_identities.user_id = user_groups.user_id WHERE group_id = ? order by identity;",
	            new SqlWorker<Boolean>() {
	                @Override
	                public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
	                	stmt.setString(1, groupId);
	        			ResultSet rs = stmt.executeQuery();
	        			while(rs.next()){
        					users.add(rs.getString(1));
	        			}
	        			return true;
	                }
	            }
	    );
		return users;
	}

	@Override
	public Share getSharePermission(String userId, String shareId) {
		// Unused
		return new Share(userId, "", Share.SharePermission.DENIED, null);
	}	
}
