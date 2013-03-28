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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import net.oauth.OAuth;
import net.oauth.OAuthAccessor;
import net.oauth.OAuthMessage;
import net.oauth.OAuthProblemException;
import net.oauth.server.OAuthServlet;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.sun.jersey.spi.container.ContainerRequest;

import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.rest.NodesController;

/**
 * Provides the REST service for /oauth/ path: the functions for retrieving OAUTH tokens
 * @author Dmitry Mishin
 */
@Path("/oauth/")

public class OauthController {
	private static final Logger logger = Logger.getLogger(OauthController.class);
	private static final Configuration conf = SettingsServlet.getConfig();

	
	@GET @POST @Path("request_token")
	public Response getRequestToken() {
		return null;
	}

	@GET @POST @Path("access_token")
	public Response getAccessToken(@Context ContainerRequest containerRequest) {
		try {
			OAuthMessage requestMessage = OAuthServlet.getMessage(containerRequest, null);
	        
	        OAuthAccessor accessor = MySQLOAuthProvider.getAccessor(requestMessage.getToken());
	        MySQLOAuthProvider.VALIDATOR.validateMessage(requestMessage, accessor);
	        
	        // make sure token is authorized
	        if (!Boolean.TRUE.equals(accessor.getProperty("authorized"))) {
	            OAuthProblemException problem = new OAuthProblemException("permission_denied");
	            throw problem;
	        }
	        // generate access token and secret
	        MySQLOAuthProvider.generateAccessToken(accessor);
	        
	        
	        ByteArrayOutputStream out = new ByteArrayOutputStream();
	        
	        OAuth.formEncode(OAuth.newList("oauth_token", accessor.accessToken,
	                                       "oauth_token_secret", accessor.tokenSecret),
	                         out);
	        ResponseBuilder response = Response.ok().
	        		type("text/plain").
	        		header("Access-Control-Allow-Origin", "*").
	        		entity(out.toByteArray());
	        
	        out.close();
	        
	    } catch (Exception e){
	    	e.printStackTrace();
	    	MySQLOAuthProvider.handleException(e, containerRequest, response, true);
	    }
	}

}
