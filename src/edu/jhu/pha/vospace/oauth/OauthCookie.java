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
package edu.jhu.pha.vospace.oauth;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import javax.servlet.http.Cookie;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/** Parse & generate the vospace_oauth cookie, which is JSON-encoded. */
public class OauthCookie {

	public static final String COOKIE_NAME = "vospace_oauth";

    private String region, requestToken, callbackUrl, shareId;

	private static final ObjectMapper mapper = new ObjectMapper();

    public static OauthCookie create(Cookie httpCookie) throws JsonParseException, JsonMappingException, UnsupportedEncodingException, IOException {
        OauthCookie result = mapper.readValue(URLDecoder.decode(httpCookie.getValue(), "ISO-8859-1"), OauthCookie.class);
        return result;
    }

    public String toString() {
        try {
           return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalStateException("Didn't expect that: " + e.getMessage(), e);
        }
    }

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}


	public String getRequestToken() {
		return requestToken;
	}

	public void setRequestToken(String requestToken) {
		this.requestToken = requestToken;
	}

	public String getCallbackUrl() {
		return callbackUrl;
	}

	public void setCallbackUrl(String callbackUrl) {
		this.callbackUrl = callbackUrl;
	}

	public String getShareId() {
		return shareId;
	}

	public void setShareId(String shareId) {
		this.shareId = shareId;
	}
}
