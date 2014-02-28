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
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;


public class SciServerAuthFilter implements ContainerRequestFilter {

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        SciServerSecurityContext sc = null;

        try {
        	KeystoneToken token = new KeystoneToken(request.getHeaderValue("X-Auth-Token"));
        	sc = new SciServerSecurityContext(token, request.isSecure()); 
        } catch (KeystoneException e) {
            throw new WebApplicationException(e.toResponse());
        }
        request.setSecurityContext(sc);
        
        if(!UserHelper.userExists(sc.getUserPrincipal().getName())) {
        	UserHelper.addDefaultUser(sc.getUserPrincipal().getName());
        }
        
        return request;
    }
}
