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
package edu.jhu.pha.vospace.storage;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;

public class SwiftJsonCredentials {
	private String username;
	private String apikey;
	private String storageurl;
	private String authtoken;
	
	private static final ObjectMapper mapper = new ObjectMapper();

    public String toString() {
        try {
           return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalStateException("Didn't expect that: " + e.getMessage(), e);
        }
    }
	
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getApikey() {
		return apikey;
	}
	public void setApikey(String apikey) {
		this.apikey = apikey;
	}
	public String getStorageurl() {
		return storageurl;
	}
	public void setStorageurl(String storageurl) {
		this.storageurl = storageurl;
	}
	public String getAuthtoken() {
		return authtoken;
	}
	public void setAuthtoken(String authtoken) {
		this.authtoken = authtoken;
	}
}
