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
package edu.jhu.pha.vospace.rest;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.regex.Pattern;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import net.oauth.OAuthAccessor;
import net.oauth.OAuthException;
import net.oauth.OAuthMessage;
import net.oauth.OAuthProblemException;
import net.oauth.server.OAuthServlet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.BadRequestException;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.PermissionDeniedException;
import edu.jhu.pha.vospace.oauth.MySQLOAuthProvider;


public class OAuthAuthenticationFilter implements ContainerRequestFilter {
	private static final Logger logger = Logger.getLogger(OAuthAuthenticationFilter.class);
	
	private @Context HttpServletRequest request;
	private static Configuration conf = SettingsServlet.getConfig();
	private static String[] noauthFilterStr = conf.getStringArray("noauth");
    
    @Override
    public ContainerRequest filter(ContainerRequest containerRequest) {
    	for(String matchStr: noauthFilterStr) {
        	if (match(pattern(matchStr), containerRequest.getPath())) {
        		return containerRequest;
    		}
    	}
    	
    	if (request.getMethod().equalsIgnoreCase("options")) {
    		return containerRequest;
		}
    	
    	String authHeader = request.getHeader("Authorization");

        java.security.cert.X509Certificate[] certs =
            (java.security.cert.X509Certificate[]) request.getAttribute(
                               "javax.servlet.request.X509Certificate");
        
        if(null != certs){
            if (certs[0] != null) {
                String dn = certs[0].getSubjectX500Principal().getName();
                try {
                  LdapName ldn = new LdapName(dn);
                  Iterator<Rdn> rdns = ldn.getRdns().iterator();
                  String org = null, cn = null;
                  while (rdns.hasNext()) {
                     Rdn rdn = (Rdn) rdns.next();
                     if (rdn.getType().equalsIgnoreCase("O"))
                         org = (String) rdn.getValue();
                     else if (rdn.getType().equalsIgnoreCase("CN"))
                         cn = (String) rdn.getValue();
                  }
                  if (cn != null){
  			        request.setAttribute("username", cn);
			        request.setAttribute("root_container", "");
                  } else {
  		            logger.error("Error authenticating the user: cn not found in certificate.");
  					throw new PermissionDeniedException("401 Unauthorized");
                  }
                    //out.println("<p>The username is:" + cn + "@" + org + "</p>");
                } catch (javax.naming.InvalidNameException e) {
                }
              }
        } else {
	    	if(null == authHeader && null == request.getParameter("oauth_version") && 
	    			(null == containerRequest.getFormParameters() || 
	    			  null == containerRequest.getFormParameters().get("oauth_version") ||
	    			  containerRequest.getFormParameters().get("oauth_version").size() == 0
        			  )){
	            throw new PermissionDeniedException("401 Unauthorized.");
	    	}
	    	
	    	try {
		        OAuthMessage requestMessage = OAuthServlet.getMessage(containerRequest, null);
		        OAuthAccessor accessor = MySQLOAuthProvider.getAccessor(requestMessage.getToken());
		        MySQLOAuthProvider.VALIDATOR.validateMessage(requestMessage, accessor);
		        String userId = (String) accessor.getProperty("user");
		        request.setAttribute("username", userId);
		        request.setAttribute("root_container", accessor.getProperty("root_container"));
		        request.setAttribute("write_permission", accessor.getProperty("write_permission"));
	    	} catch (OAuthProblemException e){
	            logger.error("Error authenticating the user: "+e.getProblem());
	            e.printStackTrace();
	            throw new PermissionDeniedException(e);
			} catch (OAuthException e) {
	            logger.error("Error authenticating the user: "+e.getMessage());
	            e.printStackTrace();
	            throw new PermissionDeniedException(e);
	        } catch (IOException e) {
	            logger.error("Error authenticating the user: "+e.getMessage());
				e.printStackTrace();
	            throw new InternalServerErrorException(e);
			} catch (URISyntaxException e) {
	            logger.error("Error authenticating the user: "+e.getMessage());
				e.printStackTrace();
	            throw new BadRequestException(e);
			}
    	}
        
        return containerRequest;
    }
    
    private static boolean match(Pattern pattern, String value) {
		return (pattern != null && value != null && pattern.matcher(value).matches());
	}
    
    private static Pattern pattern(String p) {
		if (p == null) {
			return null;
		}
		return Pattern.compile(p);
	}
}
