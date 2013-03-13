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
package edu.jhu.pha.vosync.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class InternalServerErrorException extends DropboxException {

	public InternalServerErrorException(String message) {
		super(Response.status(Status.BAD_REQUEST).entity(DropboxException.constructBody(message)).type("application/json").build());
	}

	public InternalServerErrorException(Throwable cause) {
		super(cause, Response.status(Status.BAD_REQUEST).entity(DropboxException.constructBody(cause.getMessage())).type("application/json").build());
	}

	public InternalServerErrorException(Throwable cause, String message) {
		super(cause, Response.status(Status.BAD_REQUEST).entity(DropboxException.constructBody(cause.getMessage())).type("application/json").build());
	}

}
