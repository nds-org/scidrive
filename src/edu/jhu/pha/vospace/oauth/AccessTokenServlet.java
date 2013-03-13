/*
 * Copyright 2007 AOL, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.jhu.pha.vospace.oauth;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.oauth.OAuth;
import net.oauth.OAuthAccessor;
import net.oauth.OAuthMessage;
import net.oauth.OAuthProblemException;
import net.oauth.server.OAuthServlet;

/**
 * Access Token request handler
 *
 * @author Praveen Alavilli
 */
public class AccessTokenServlet extends HttpServlet {
    
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        // nothing at this point
    }
    
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        processRequest(request, response);
    }
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        processRequest(request, response);
    }
        
    @Override
    protected void doOptions(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        //response.setHeader("Access-Control-Max-Age", "0");
        //response.setHeader("Access-Control-Allow-Methods", "PUT, GET, POST, HEAD, OPTIONS");
        response.setHeader("Access-Control-Allow-Headers", "authorization,x-requested-with");
        super.doOptions(request, response);
    }
    
    public void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        try{
            OAuthMessage requestMessage = OAuthServlet.getMessage(request, null);
            
            OAuthAccessor accessor = MySQLOAuthProvider.getAccessor(requestMessage.getToken());
            MySQLOAuthProvider.VALIDATOR.validateMessage(requestMessage, accessor);
            
            // make sure token is authorized
            if (!Boolean.TRUE.equals(accessor.getProperty("authorized"))) {
                OAuthProblemException problem = new OAuthProblemException("permission_denied");
                throw problem;
            }
            // generate access token and secret
            MySQLOAuthProvider.generateAccessToken(accessor);
            
            response.setContentType("text/plain");
            response.setHeader("Access-Control-Allow-Origin", "*");
            OutputStream out = response.getOutputStream();
            OAuth.formEncode(OAuth.newList("oauth_token", accessor.accessToken,
                                           "oauth_token_secret", accessor.tokenSecret),
                             out);
            out.close();
            
        } catch (Exception e){
        	e.printStackTrace();
        	MySQLOAuthProvider.handleException(e, request, response, true);
        }
    }

    private static final long serialVersionUID = 1L;

}
