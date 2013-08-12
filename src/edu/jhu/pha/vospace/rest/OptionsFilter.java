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

import javax.ws.rs.core.Response.Status;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;


public class OptionsFilter implements ContainerResponseFilter {
	
	@Override
	public ContainerResponse filter(ContainerRequest request,
			ContainerResponse response) {

		if(request.getMethod().equals("OPTIONS")){
			response.getHttpHeaders().add("Access-Control-Allow-Origin", "*");
			response.getHttpHeaders().add("Access-Control-Allow-Headers", "Authorization, X-Requested-With, Content-Type");
			response.getHttpHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE, PUT");
		} else {
			response.getHttpHeaders().add("Access-Control-Allow-Origin", "*");
		}
		
		if(response.getStatus() == Status.UNAUTHORIZED.getStatusCode()){
			response.getHttpHeaders().add("WWW-Authenticate", "OAuth realm=\""+request.getBaseUri().getScheme()+"://"+request.getBaseUri().getHost()+"\"");
		}
		
		response.getHttpHeaders().add("Cache-Control", "no-cache");
		response.getHttpHeaders().add("Expires", "-1");
		return response;
	}
}
