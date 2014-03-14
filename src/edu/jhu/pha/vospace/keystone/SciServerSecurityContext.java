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

import java.security.Principal;
import java.util.HashMap;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.meta.MetaStoreFactory;
import edu.jhu.pha.vospace.meta.Share;
import edu.jhu.pha.vospace.oauth.SciDriveUser;

public class SciServerSecurityContext implements SecurityContext {
    private final KeystoneToken token;
    private final boolean isSecure;
    private final SciDriveUser user;
	private static Logger logger = Logger.getLogger(SciServerSecurityContext.class); 

    public SciServerSecurityContext(KeystoneToken token, String shareId, boolean isSecure) {
        this.token = token;
        this.isSecure = isSecure;
        HashMap<String, String> storageCredentials = new HashMap<String, String>();
        if(null != token.getSwiftAccountUrl())
        	storageCredentials.put("storageurl", token.getSwiftAccountUrl());
        
    	if(null != shareId) {
    		Share share = MetaStoreFactory.getUserHelper().getSharePermission(token.getUserId(), shareId);
    		if(share.getPermission().equals(Share.SharePermission.DENIED))
    			throw new KeystoneException(Response.Status.UNAUTHORIZED, "Denied");
    		this.user = new SciDriveUser(share.getUserId(), share.getContainer(), share.getPermission().canWrite(), storageCredentials);
    	} else {
            this.user = new SciDriveUser(token.getUserId(), "", true, storageCredentials);
    	}
    	
        ObjectMapper mapper = new ObjectMapper();
        
        if(MetaStoreFactory.getUserHelper().getDataStoreCredentials(this.user.getName()).isEmpty())
			try {
				MetaStoreFactory.getUserHelper().setDataStoreCredentials(this.user.getName(), mapper.writer().writeValueAsString(storageCredentials));
			} catch (Exception e) {
				logger.error("Error writing user storage credentials to DB");
				e.printStackTrace();
			}
        
    }

    @Override
    public Principal getUserPrincipal() {
    	return this.user;
    }

    @Override
    public boolean isUserInRole(String role) {
        return token.getRoles().contains(role);
    }

    @Override
    public boolean isSecure() {
        return isSecure;
    }

    @Override
    public String getAuthenticationScheme() {
        return "Keystone";
    }

}
