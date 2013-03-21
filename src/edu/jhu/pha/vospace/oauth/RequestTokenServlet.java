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
import net.oauth.OAuthConsumer;
import net.oauth.OAuthMessage;
import net.oauth.server.OAuthServlet;

import org.apache.log4j.Logger;

/**
 * Request token request handler
 * 
 * @author Praveen Alavilli, Dmitry Mishin
 */
public class RequestTokenServlet extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1683210755941800627L;
	private static final Logger logger = Logger.getLogger(RequestTokenServlet.class);
    
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
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
        response.setHeader("Access-Control-Allow-Methods", "PUT, GET, POST, HEAD, OPTIONS");
        response.setHeader("Access-Control-Allow-Headers", "authorization,x-requested-with");
        super.doOptions(request, response);
    }
    
    public void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        try {
            OAuthMessage requestMessage = OAuthServlet.getMessage(request, null);
            
            OAuthConsumer consumer = MySQLOAuthProvider.getConsumer(requestMessage.getConsumerKey());
            
            OAuthAccessor accessor = new OAuthAccessor(consumer);
            MySQLOAuthProvider.VALIDATOR.validateMessage(requestMessage, accessor);
            {
                // Support the 'Variable Accessor Secret' extension
                // described in http://oauth.pbwiki.com/AccessorSecret
                String secret = requestMessage.getParameter("oauth_accessor_secret");
                if (secret != null) {
                    accessor.setProperty(OAuthConsumer.ACCESSOR_SECRET, secret);
                }
            }
            // generate request_token and secret
            if(null != request.getParameter("share")){
            	MySQLOAuthProvider.generateRequestToken(accessor, request.getParameter("share"));
            } else {
            	MySQLOAuthProvider.generateRequestToken(accessor);
            }
            
            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setContentType("text/plain");
            OutputStream out = response.getOutputStream();
            OAuth.formEncode(OAuth.newList("oauth_token", accessor.requestToken,
                                           "oauth_token_secret", accessor.tokenSecret),
                             out);
            out.close();
        } catch (Exception e){
        	e.printStackTrace();
        	MySQLOAuthProvider.handleException(e, request, response, true);
        }
        
    }
}
