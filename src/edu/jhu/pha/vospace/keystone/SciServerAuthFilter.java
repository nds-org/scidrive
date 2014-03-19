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

import javax.ws.rs.WebApplicationException;

import org.apache.log4j.Logger;

import com.sun.jersey.oauth.signature.OAuthParameters;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import edu.jhu.pha.vospace.meta.MetaStoreFactory;
import edu.jhu.pha.vospace.oauth.SciDriveUser;


public class SciServerAuthFilter implements ContainerRequestFilter {

	private static Logger logger = Logger.getLogger(SciServerAuthFilter.class); 

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        String authHeader = request.getHeaderValue("X-Auth-Token");
        String share = request.getHeaderValue("X-Share");

        if (authHeader == null && share == null) {
        	logger.error("Not found authHeader and shareHeader");
            return request;
        }

    	SciServerSecurityContext sc = null;

        if(request.getMethod().equals("OPTIONS"))
        	return request;
        
    	logger.debug("Continue;");
        
        try {
        	KeystoneToken token = null;
        	if(null != authHeader)
        		token = new KeystoneToken(authHeader);
        	
        	sc = new SciServerSecurityContext(token, share, request.isSecure()); 
        } catch (KeystoneException e) {
            throw new WebApplicationException(e.toResponse());
        }
        request.setSecurityContext(sc);
        
        if(!MetaStoreFactory.getUserHelper().userExists((SciDriveUser)sc.getUserPrincipal())) {
        	MetaStoreFactory.getUserHelper().addDefaultUser((SciDriveUser)sc.getUserPrincipal());
        }
        
        return request;
    }
}
