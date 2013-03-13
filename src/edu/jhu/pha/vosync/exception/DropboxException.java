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

import java.io.ByteArrayOutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

public abstract class DropboxException extends WebApplicationException {
	private static final long serialVersionUID = -8345838864718847104L;
	static final JsonFactory fact = new JsonFactory();
	private static final Logger logger = Logger.getLogger(DropboxException.class);

	public DropboxException(Response response) {
		super(response);
	}
	
	public DropboxException(Throwable cause) {
		super(cause);
	}
	
	public DropboxException(Throwable cause, Response response) {
		super(cause, response);
	}
	
	static byte[] constructBody(String message) {
    	ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    	try {
			JsonGenerator g = fact.createJsonGenerator(byteOut).useDefaultPrettyPrinter();
			
			g.writeStartObject();
			g.writeStringField("error", message);
			g.writeEndObject();
			
			g.close();
			byteOut.close();
    	} catch (Exception ex) {
    		logger.error("Error constructing DropboxException: "+ex.getMessage());
    	}
		return byteOut.toByteArray();
	}


}
