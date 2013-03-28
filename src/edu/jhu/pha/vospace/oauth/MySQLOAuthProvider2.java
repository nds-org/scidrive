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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Vector;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.oauth.server.spi.OAuthConsumer;
import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.rest.VoboxOAuthProvider.Consumer;

/**
 *
 * @author Dmitry Mishin
 */
public class MySQLOAuthProvider2 {

    //public static final OAuthValidator VALIDATOR = new SimpleOAuthValidator();

	private static final Logger logger = Logger.getLogger(MySQLOAuthProvider.class);

	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	static {
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	
    public static synchronized Token getRequestToken(final String tokenStr) {
        
    	Token tokenObj = DbPoolServlet.goSql("Get oauth token",
        		"select request_token, token_secret, consumer_key, callback_url, identity, container_name, accessor_write_permission "+
        				"from oauth_accessors "+
        				"join oauth_consumers on oauth_consumers.consumer_id = oauth_accessors.consumer_id "+
        				"left outer join containers on containers.container_id = oauth_accessors.container_id "+
        				"left outer join users on users.user_id = containers.user_id "+
        				"left outer join user_identities on users.user_id = user_identities.user_id "+
        				"where request_token = ? limit 1",
                new SqlWorker<Token>() {
                    @Override
                    public Token go(Connection conn, PreparedStatement stmt) throws SQLException {
                        Token token = null;

                        stmt.setString(1, tokenStr);
                        stmt.setString(2, tokenStr);
                        ResultSet rs = stmt.executeQuery();
            			if(rs.next()){
            	            token = new Token(
            	                    rs.getString("request_token"), 
            	                    rs.getString("token_secret"), 
            	                    rs.getString("consumer_key"), 
            	                    rs.getString("callback_url"), 
            	                    null);
            			}
            			
            			return token;
                    }
                }
        );
        
        return tokenObj;
        
    }

	
    public static synchronized Token getAccessToken(final String tokenStr) {
        logger.debug("Get access token");
    	Token tokenObj = DbPoolServlet.goSql("Get oauth token",
        		"select access_token, token_secret, consumer_key, callback_url, identity, container_name, accessor_write_permission "+
        				"from oauth_accessors "+
        				"join oauth_consumers on oauth_consumers.consumer_id = oauth_accessors.consumer_id "+
        				"left outer join containers on containers.container_id = oauth_accessors.container_id "+
        				"left outer join users on users.user_id = containers.user_id "+
        				"left outer join user_identities on users.user_id = user_identities.user_id "+
        				"where access_token = ? limit 1",
                new SqlWorker<Token>() {
                    @Override
                    public Token go(Connection conn, PreparedStatement stmt) throws SQLException {
                        Token token = null;

                        stmt.setString(1, tokenStr);
                        ResultSet rs = stmt.executeQuery();
            			if(rs.next()){
            	            token = new Token(
            	                    rs.getString("access_token"), 
            	                    rs.getString("token_secret"), 
            	                    rs.getString("consumer_key"), 
            	                    rs.getString("callback_url"), 
            	                    new MultivaluedMapImpl());
            			}
            			return token;
                    }
                }
        );
        
        return tokenObj;
        
    }

	
	
	
	
    public static synchronized OAuthConsumer getConsumer(final String consumer_key) {
        logger.debug("Get consumer");
    	return DbPoolServlet.goSql("Get oauth consumer",
        		"select callback_url, consumer_key, consumer_secret, consumer_description, container from oauth_consumers where consumer_key = ?",
                new SqlWorker<Consumer>() {
                    @Override
                    public Consumer go(Connection conn, PreparedStatement stmt) throws SQLException {
                        Consumer consumer = null;

                        stmt.setString(1, consumer_key);
                        ResultSet rs = stmt.executeQuery();
            			if(rs.next()){
            	            consumer = new Consumer(
            	                    rs.getString("callback_url"), 
            	                    rs.getString("consumer_key"), 
            	                    rs.getString("consumer_secret"), 
            	                    new MultivaluedMapImpl());
                            consumer.getAttributes().add("name", rs.getString("consumer_key"));
                            consumer.getAttributes().add("description", rs.getString("consumer_description"));
                            consumer.getAttributes().add("container", rs.getString("container"));
            			}
            			
            			return consumer;
                    }
                }
        );
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
    public static synchronized Token generateRequestToken(final String consumer_key, final String callback_url, MultivaluedMap<String, String> attributes) {
    	
        // generate token and secret based on consumer_key
        
        // for now use md5 of name + current time as token
        final String token_data = consumer_key + System.nanoTime();
        final String token = DigestUtils.md5Hex(token_data);
        // for now use md5 of name + current time + token as secret
        final String secret_data = consumer_key + System.nanoTime() + token;
        final String secret = DigestUtils.md5Hex(secret_data);
        
        Token tokenObj = new Token(token, secret, consumer_key, callback_url, attributes);
        
        DbPoolServlet.goSql("Insert new request token",
        		"insert into oauth_accessors (request_token, token_secret, consumer_id, created) select ?, ?, consumer_id , ? from oauth_consumers where consumer_key = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
            			stmt.setString(1, token);
            			stmt.setString(2, secret);
            			stmt.setString(3, dateFormat.format(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime()));
            			stmt.setString(4, consumer_key);
                        return stmt.execute();
                    }
                }
        );
        return tokenObj;
    }
    
    /**
     * Generate a fresh request token and secret for a consumer.
     * 
     * @throws OAuthException
     */
    public static synchronized Token generateAccessToken(final Token requestToken, String verifier) {

        // generate oauth_token and oauth_secret
        final String consumer_key = (String) requestToken.getConsumer().getKey();
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
            			stmt.setString(3, requestToken.getToken());
                        return stmt.execute();
                    }
                }
        );

        return new Token(token, 
        		requestToken.getSecret(), 
        		requestToken.getConsumer().getKey(), 
        		requestToken.getCallbackUrl(),
        		requestToken.getAttributes()
        		);
    }

}
