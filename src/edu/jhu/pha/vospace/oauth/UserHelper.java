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
import java.net.URISyntaxException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.api.AccountInfo;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.PermissionDeniedException;
import edu.jhu.pha.vospace.node.ContainerNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.storage.StorageManager;
import edu.jhu.pha.vospace.storage.StorageManagerFactory;
import edu.jhu.pha.vospace.storage.SwiftJsonCredentials;

public class UserHelper {
	private static final Logger logger = Logger.getLogger(UserHelper.class);
	
    public static void updateUser(final String username, final Map<UserField, String> field_vals) {
    	if(field_vals.size() == 0)
    		return;
    	
    	StringBuffer queryFields = new StringBuffer();
    	for(UserField fieldType: field_vals.keySet()) {
    		if(queryFields.length() > 0)
    			queryFields.append(",");
    		queryFields.append(fieldType.getDbField()+" = ? ");
    	}
    	
        DbPoolServlet.goSql("setting fields for " + username,
            "update user_identities set "+queryFields+" where identity = ?",
            new SqlWorker<Boolean>() {
                @Override
                public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                	int fieldCount=1;
                	for(UserField fieldType: field_vals.keySet()) {
                		stmt.setString(fieldCount++, field_vals.get(fieldType));
                	}
            		stmt.setString(fieldCount++, username);
                    stmt.executeUpdate();
                    return true;
                }
            });
    }

    /** Retrieve a user's certificate as a blob from the database. Null if it doesn't exist.
     *  If the user doesn't exist, throws an IllegalStateException (so you should call userExists first). */
    public static Blob getCertificate(final String username) {
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

	public static SwiftJsonCredentials getDataStoreCredentials(final String username) {
        return DbPoolServlet.goSql("Retrieving credentials for " + username,
                "select storage_credentials from users JOIN user_identities ON users.user_id = user_identities.user_id where identity = ?;",
                new SqlWorker<SwiftJsonCredentials>() {
                    @Override
                    public SwiftJsonCredentials go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username);
                        ResultSet rs = stmt.executeQuery();
                        if (!rs.next())
                            throw new PermissionDeniedException("The user "+username+" does not exist.");
                        
                    	ObjectMapper mapper = new ObjectMapper();
                    	try {
                    		SwiftJsonCredentials result = mapper.readValue(rs.getBytes("storage_credentials"), SwiftJsonCredentials.class);
	                        return result;
	                    } catch(IOException ex) {
	                    	throw new InternalServerErrorException("Unable to read user "+username+" storage credentials");
	                    }
                    }
                });
	}

	/**
	 * Returns all processors credentials from the DB
	 * @param username
	 * @param processorId
	 * @return
	 */
	public static JsonNode getProcessorCredentials(final String username) {
        return DbPoolServlet.goSql("Retrieving processor credentials for " + username,
                "select service_credentials from users JOIN user_identities ON users.user_id = user_identities.user_id where identity = ?;",
                new SqlWorker<JsonNode>() {
                    @Override
                    public JsonNode go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username);
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

	public static int setDataStoreCredentials(final String username, final String credentials) {
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

    static public boolean addDefaultUser(final String username) {
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
	                                        	stmt.setString(2, username);
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
            			stmt.setString(2, username);
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

    
	public static AccountInfo getAccountInfo(final String username) {
		AccountInfo info = DbPoolServlet.goSql("Getting user \"" + username + "\" limits from DB.",
                "select hardlimit, softlimit from users JOIN user_identities ON users.user_id = user_identities.user_id WHERE identity = ?;",
                new SqlWorker<AccountInfo>() {
                    @Override
                    public AccountInfo go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username);
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()){
                            AccountInfo info = new AccountInfo();
                            info.setUsername(username);
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
    public static boolean userExists(final String username) {
        return DbPoolServlet.goSql("Checking whether user \"" + username + "\" exists in DB.",
                "select count(identity) from user_identities where identity = ?;",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username);
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next())
                            return rs.getInt(1) > 0;
                        else
                            throw new IllegalStateException("No result from query.");
                    }
                }
        );
    }
    
    /**
     * Modifies the user credentials for metadata extractors.
     * @param username The user ID
     * @param processorId processor ID (from processors.xml/processor/id)
     * @param updateNode The processor credentials in JSON format. If null, the processor will be disabled
     * @return true if success
     */
    public static boolean updateUserService(final String username, final String processorId, JsonNode updateNode) {
        byte[] curNode = DbPoolServlet.goSql("Retrieving user's service credentials from db",
                "select service_credentials from users JOIN user_identities ON users.user_id = user_identities.user_id where identity = ?;",
                new SqlWorker<byte[]>() {
                    @Override
                    public byte[] go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username);
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
                    stmt.setString(2, username);
                    return stmt.execute();
                }
            }
        );
    
    }

	public static JsonNode getUserServices(final String username) {
        byte[] curNode = DbPoolServlet.goSql("Retrieving user's service credentials from db",
                "select service_credentials from users JOIN user_identities ON users.user_id = user_identities.user_id where identity = ?;",
                new SqlWorker<byte[]>() {
                    @Override
                    public byte[] go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, username);
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

	public enum UserField {
		EMAIL("email", "http://schema.openid.net/contact/email"), 
		FIRST_NAME("first_name", "http://schema.openid.net/namePerson/first"), 
		LAST_NAME("last_name", "http://schema.openid.net/namePerson/last"),
		ORGANIZATION("organization", "http://sso.usvao.org/schema/institution");
		private String dbField;
		private String openidField;
		private UserField(String dbField, String openidField) 	{
			this.dbField = dbField;	
			this.openidField = openidField;	
		}
		public String getDbField() 			{	return this.dbField;	}
		public String getOpenidField() 		{	return this.openidField;	}
	};

}
