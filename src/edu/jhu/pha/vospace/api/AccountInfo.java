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
package edu.jhu.pha.vospace.api;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;

public class AccountInfo {
	private static final ObjectMapper mapper = new ObjectMapper();
	private String username;
	private long bytesUsed;
	private int softLimit;
	private int hardLimit;

	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public long getBytesUsed() {
		return bytesUsed;
	}
	public void setBytesUsed(long bytesUsed) {
		this.bytesUsed = bytesUsed;
	}
	public int getSoftLimit() {
		return softLimit;
	}
	public void setSoftLimit(int softLimit) {
		this.softLimit = softLimit;
	}
	public int getHardLimit() {
		return hardLimit;
	}
	public void setHardLimit(int hardLimit) {
		this.hardLimit = hardLimit;
	}
	
	public String toJson() {
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonGenerationException e) {
			throw new InternalServerErrorException(e, "Error serializing account info to JSON");
		} catch (JsonMappingException e) {
			throw new InternalServerErrorException(e, "Error serializing account info to JSON");
		} catch (IOException e) {
			throw new InternalServerErrorException(e, "Error serializing account info to JSON");
		}
	}
}
