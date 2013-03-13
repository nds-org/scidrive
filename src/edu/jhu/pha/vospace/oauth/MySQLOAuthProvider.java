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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Vector;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.oauth.OAuthAccessor;
import net.oauth.OAuthConsumer;
import net.oauth.OAuthException;
import net.oauth.OAuthMessage;
import net.oauth.OAuthProblemException;
import net.oauth.OAuthValidator;
import net.oauth.SimpleOAuthValidator;
import net.oauth.server.OAuthServlet;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.meta.MetaStore;
import edu.jhu.pha.vospace.meta.MetaStoreFactory;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;

/**
 *
 * @author Dmitry Mishin
 */
public class MySQLOAuthProvider {

    public static final OAuthValidator VALIDATOR = new SimpleOAuthValidator();

	private static final Logger logger = Logger.getLogger(MySQLOAuthProvider.class);

	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	static {
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
    public static synchronized OAuthConsumer getConsumer(final String consumer_key)
            throws IOException, OAuthProblemException {
        
    	OAuthConsumer consumer = DbPoolServlet.goSql("Get oauth consumer",
        		"select callback_url, consumer_key, consumer_secret, consumer_description, container from oauth_consumers where consumer_key = ?",
                new SqlWorker<OAuthConsumer>() {
                    @Override
                    public OAuthConsumer go(Connection conn, PreparedStatement stmt) throws SQLException {
                        OAuthConsumer consumer = null;

                        stmt.setString(1, consumer_key);
                        ResultSet rs = stmt.executeQuery();
            			if(rs.next()){
            	            consumer = new OAuthConsumer(
            	                    rs.getString("callback_url"), 
            	                    rs.getString("consumer_key"), 
            	                    rs.getString("consumer_secret"), 
            	                    null);
                            consumer.setProperty("name", rs.getString("consumer_key"));
                            consumer.setProperty("description", rs.getString("consumer_description"));
                            consumer.setProperty("container", rs.getString("container"));
            			}
            			
            			return consumer;
                    }
                }
        );
        
        if(consumer == null) {
        	logger.error("Not found consumer " + consumer_key);
            OAuthProblemException problem = new OAuthProblemException("token_rejected");
            throw problem;
        }

        return consumer;
        
    }
    
    /**
     * Get the access token and token secret for the given oauth_token. 
     */
    public static synchronized OAuthAccessor getAccessor(final String consumer_token)
            throws IOException, OAuthProblemException {
        
        // try to load from local cache if not throw exception
        //final String consumer_token = requestMessage.getToken();

    	OAuthAccessor accessor = DbPoolServlet.goSql("Get oauth accessor",
        		"select access_token, request_token, token_secret, consumer_key, authorized, identity, container_name, accessor_write_permission "+
        				"from oauth_accessors "+
        				"join oauth_consumers on oauth_consumers.consumer_id = oauth_accessors.consumer_id "+
        				"left outer join containers on containers.container_id = oauth_accessors.container_id "+
        				"left outer join users on users.user_id = containers.user_id "+
        				"left outer join user_identities on users.user_id = user_identities.user_id "+
        				"where request_token = ? or access_token = ? limit 1",
                new SqlWorker<OAuthAccessor>() {
                    @Override
                    public OAuthAccessor go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	OAuthAccessor accessor = null;

            			stmt.setString(1, consumer_token);
            			stmt.setString(2, consumer_token);
            			ResultSet resSet = stmt.executeQuery();
            			
            			if(resSet.next()){
            				try {
								accessor = new OAuthAccessor(getConsumer(resSet.getString("consumer_key")));
	            				accessor.accessToken = resSet.getString("access_token");
	            				accessor.requestToken = resSet.getString("request_token");
	            				accessor.tokenSecret = resSet.getString("token_secret");
	            				accessor.setProperty("user", resSet.getString("identity"));
	            				accessor.setProperty("authorized", resSet.getBoolean("authorized"));
	            				accessor.setProperty("root_container", resSet.getString("container_name"));
	            				accessor.setProperty("write_permission", resSet.getBoolean("accessor_write_permission"));
							} catch (OAuthProblemException e) {
								logger.error("Error getting oauth accessor: "+e.getMessage());
							} catch (IOException e) {
								logger.error("Error getting oauth accessor: "+e.getMessage());
							}
            			}            			
            			return accessor;
                    }
                }
        );

        if(accessor == null){
        	logger.error("Error getting the accessor info from the MySQL database for token " + consumer_token);
            OAuthProblemException problem = new OAuthProblemException("token_expired");
            throw problem;
        }
        
        return accessor;
    }


    /**
     * Set the access token
     * If userId != null, creates link to user's container as root matching the name in consumer. The container should exist already.
     * @param accessor The OAuth accessor object
     * @param userId the owner userId
     * @throws OAuthException
     */
    public static synchronized void markAsAuthorized(final OAuthAccessor accessor, final String userId)
            throws OAuthException {
    	
		try {
			if(null == (String)accessor.getProperty("root_container")) { // No predefined one (can be predefined for sharing); in this case set the default one
				Object default_root_container = accessor.consumer.getProperty("container");
				accessor.setProperty("root_container", (null == default_root_container)?"":default_root_container);

		        if (!UserHelper.userExists(userId)) {
		            UserHelper.addDefaultUser(userId);
		        }
				
				//First check if the root node exists
				VospaceId identifier = new VospaceId(new NodePath((String)accessor.getProperty("root_container")));
				Node node = NodeFactory.getInstance().createNode(identifier, userId, NodeType.CONTAINER_NODE);
				logger.debug("Marking as authorized, root node: "+identifier.toString());
				if(!node.isStoredMetadata()){
					node.setNode(null);
					logger.debug("Creating the node "+node.getUri());
				}
	
		        DbPoolServlet.goSql("Mark oauth token as authorized",
		        		"update oauth_accessors set container_id = (select container_id from containers join user_identities on containers.user_id = user_identities.user_id where identity = ? and container_name = ?), authorized = 1 where request_token = ?;",
		                new SqlWorker<Integer>() {
		                    @Override
		                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
		            			stmt.setString(1, userId);
		            			stmt.setString(2, (String)accessor.getProperty("root_container"));
		            			stmt.setString(3, accessor.requestToken);
		                        return stmt.executeUpdate();
		                    }
		                }
		        );
		        accessor.setProperty("user", userId);
			} else { // the container is already set up (sharing)
	            DbPoolServlet.goSql("Mark oauth token as authorized",
	            		"update oauth_accessors set authorized = 1 where request_token = ?;",
	                    new SqlWorker<Integer>() {
	                        @Override
	                        public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
	                			stmt.setString(1, accessor.requestToken);
	                            return stmt.executeUpdate();
	                        }
	                    }
	            );
			}
	        accessor.setProperty("authorized", Boolean.TRUE);
		} catch(URISyntaxException ex) {
			logger.error("Error creating root (app) node for user: "+ex.getMessage());
		}
    	
    }

    /**
     * Returns the user the share is made for
     * @param shareId
     * @return
     */
    public static synchronized Vector<String> getShareUsers(final String shareId) {
        return DbPoolServlet.goSql("Get share user logins",
        		"select identity from user_identities JOIN user_groups ON user_groups.user_id = user_identities.user_id JOIN container_shares ON user_groups.group_id = container_shares.group_id AND container_shares.share_id = ?",
                new SqlWorker<Vector<String>>() {
                    @Override
                    public Vector<String> go(Connection conn, PreparedStatement stmt) throws SQLException {
            			stmt.setString(1, shareId);
            			Vector<String> returnVect = new Vector<String>();
            			ResultSet rs = stmt.executeQuery();
            			while(rs.next()){
            				returnVect.add(rs.getString(1));
            			}
            			return returnVect;
                    }
                }
        );
    }

    /**
     * Generate a fresh request token and secret for a consumer.
     * 
     * @throws OAuthException
     */
    public static synchronized void generateRequestToken(
            final OAuthAccessor accessor)
            throws OAuthException {
    	
        // generate oauth_token and oauth_secret
        String consumer_key = (String) accessor.consumer.getProperty("name");
        // generate token and secret based on consumer_key
        
        // for now use md5 of name + current time as token
        final String token_data = consumer_key + System.nanoTime();
        final String token = DigestUtils.md5Hex(token_data);
        // for now use md5 of name + current time + token as secret
        final String secret_data = consumer_key + System.nanoTime() + token;
        final String secret = DigestUtils.md5Hex(secret_data);
        
        accessor.requestToken = token;
        accessor.tokenSecret = secret;
        accessor.accessToken = null;
        
        DbPoolServlet.goSql("Insert new request token",
        		"insert into oauth_accessors (request_token, token_secret, consumer_id, created) select ?, ?, consumer_id , ? from oauth_consumers where consumer_key = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
            			stmt.setString(1, token);
            			stmt.setString(2, secret);
            			stmt.setString(3, dateFormat.format(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime()));
            			stmt.setString(4, accessor.consumer.consumerKey);
                        return stmt.execute();
                    }
                }
        );
    }

    /**
     * Generate a fresh request token and secret for a consumer.
     * 
     * @throws OAuthException
     */
    public static synchronized void generateRequestToken(
            final OAuthAccessor accessor, final String shareId)
            throws OAuthException {
    	
        // generate oauth_token and oauth_secret
        String consumer_key = (String) accessor.consumer.getProperty("name");
        // generate token and secret based on consumer_key
        
        // for now use md5 of name + current time as token
        final String token_data = consumer_key + System.nanoTime();
        final String token = DigestUtils.md5Hex(token_data);
        // for now use md5 of name + current time + token as secret
        final String secret_data = consumer_key + System.nanoTime() + token;
        final String secret = DigestUtils.md5Hex(secret_data);
        
        accessor.requestToken = token;
        accessor.tokenSecret = secret;
        accessor.accessToken = null;
        
        DbPoolServlet.goSql("Insert new request token",
        		"insert into oauth_accessors (request_token, token_secret, consumer_id, container_id, created, accessor_write_permission) "+
        				"select ?, ?, consumer_id , container_id, ?, share_write_permission from oauth_consumers, container_shares "+
        				"where consumer_key = ? and share_id = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
            			stmt.setString(1, token);
            			stmt.setString(2, secret);
            			stmt.setString(3, dateFormat.format(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime()));
            			stmt.setString(4, accessor.consumer.consumerKey);
            			stmt.setString(5, shareId);
                        return stmt.execute();
                    }
                }
        );
    }
    
    /**
     * Generate a fresh request token and secret for a consumer.
     * 
     * @throws OAuthException
     */
    public static synchronized void generateAccessToken(final OAuthAccessor accessor)
            throws OAuthException {

        // generate oauth_token and oauth_secret
        final String consumer_key = (String) accessor.consumer.getProperty("name");
        // generate token and secret based on consumer_key
        
        // for now use md5 of name + current time as token
        final String token_data = consumer_key + System.nanoTime();
        final String token = DigestUtils.md5Hex(token_data);
        // first remove the accessor from cache
        
        DbPoolServlet.goSql("Insert new access token",
        		"update oauth_accessors set request_token = NULL, access_token = ?, created = ? where request_token = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
            			stmt.setString(1, token);
            			stmt.setString(2, dateFormat.format(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime()));
            			stmt.setString(3, accessor.requestToken);
                        return stmt.execute();
                    }
                }
        );

        accessor.requestToken = null;
        accessor.accessToken = token;
    }


    public static void handleException(Exception e, HttpServletRequest request,
            HttpServletResponse response, boolean sendBody)
            throws IOException, ServletException {
        String realm = (request.isSecure())?"https://":"http://";
        realm += request.getLocalName();
        OAuthServlet.handleException(response, e, realm, sendBody); 
    }

}
