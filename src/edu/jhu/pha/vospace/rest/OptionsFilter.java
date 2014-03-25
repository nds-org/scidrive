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

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response.Status;

public class OptionsFilter implements ContainerResponseFilter {
	
	@Override
	public void filter(ContainerRequestContext request,
			ContainerResponseContext response) throws IOException {

		if(request.getMethod().equals("OPTIONS")){
			response.getHeaders().add("Access-Control-Allow-Origin", "*");
			response.getHeaders().add("Access-Control-Allow-Headers", request.getHeaderString("Access-Control-Request-Headers"));
			response.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE, PUT");
			response.getHeaders().add("Access-Control-Max-Age", "10");
		} else {
			response.getHeaders().add("Access-Control-Allow-Origin", "*");
		}
		
		if(response.getStatus() == Status.UNAUTHORIZED.getStatusCode()){
//			response.getHttpHeaders().add("WWW-Authenticate", "OAuth realm=\""+request.getBaseUri().getScheme()+"://"+request.getBaseUri().getHost()+"\"");
			response.getHeaders().add("WWW-Authenticate", "Keystone realm=\""+request.getUriInfo().getBaseUri().getScheme()+"://"+request.getUriInfo().getBaseUri().getHost()+"\"");
		}
		
		response.getHeaders().add("Cache-Control", "no-cache");
		response.getHeaders().add("Expires", "-1");
		return;
	}

}
