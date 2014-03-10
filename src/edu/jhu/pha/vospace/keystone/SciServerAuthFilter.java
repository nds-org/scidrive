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

import com.sun.jersey.oauth.signature.OAuthParameters;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import edu.jhu.pha.vospace.oauth.SciDriveUser;


public class SciServerAuthFilter implements ContainerRequestFilter {

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        String authHeader = request.getHeaderValue("X-Auth-Token");
        if (authHeader == null) {
            return request;
        }

    	SciServerSecurityContext sc = null;

        if(request.getMethod().equals("OPTIONS"))
        	return request;
        
        try {
        	KeystoneToken token = new KeystoneToken(request.getHeaderValue("X-Auth-Token"));
        	sc = new SciServerSecurityContext(token, request.isSecure()); 
        } catch (KeystoneException e) {
            throw new WebApplicationException(e.toResponse());
        }
        request.setSecurityContext(sc);
        
        if(!UserHelper.userExists((SciDriveUser)sc.getUserPrincipal())) {
        	UserHelper.addDefaultUser((SciDriveUser)sc.getUserPrincipal());
        }
        
        return request;
    }
}