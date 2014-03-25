package edu.jhu.pha.vospace.rest;

import java.security.Principal;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;
import org.glassfish.jersey.server.oauth1.OAuth1Consumer;
import org.glassfish.jersey.server.oauth1.OAuth1Provider;
import org.glassfish.jersey.server.oauth1.OAuth1Token;

import edu.jhu.pha.vospace.oauth.MySQLOAuthProvider2;
import edu.jhu.pha.vospace.oauth.Token;

@Provider
public class SciDriveOAuthProvider implements OAuth1Provider {

    @Override
    public OAuth1Consumer getConsumer(String consumerKey) {
    	return MySQLOAuthProvider2.getConsumer(consumerKey);
    }

    @Override
    public OAuth1Token getRequestToken(String token) {
        return MySQLOAuthProvider2.getRequestToken(token);
    }

    @Override
    public OAuth1Token newRequestToken(String consumerKey, String callbackUrl, Map<String, List<String>> attributes) {
        return MySQLOAuthProvider2.generateRequestToken(consumerKey, callbackUrl, mapToMulti(attributes));
    }

    private static MultivaluedMap<String, String> mapToMulti(Map<String, List<String>> map) {
    	MultivaluedMap<String, String> newMap = new MultivaluedStringMap();
    	for(String key: map.keySet()) {
    		List<String> values = map.get(key);
    		for(String value: values) {
    			newMap.add(key, value);
    		}
    	}
    	return newMap;
    }
    
    @Override
    public OAuth1Token newAccessToken(OAuth1Token requestToken, String verifier) {
    	return MySQLOAuthProvider2.generateAccessToken((Token)requestToken, verifier);
    }

    @Override
    public OAuth1Token getAccessToken(String token) {
        return MySQLOAuthProvider2.getAccessToken(token);
    }

    public static class Consumer implements OAuth1Consumer {
        private final String key;
        private final String secret;
        private final MultivaluedMap<String, String> attribs;

        public Consumer(String key, String secret, MultivaluedMap<String, String> attributes) {
            this.key = key;
            this.secret = secret;
            this.attribs = attributes;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getSecret() {
            return secret;
        }

        public MultivaluedMap<String, String> getAttributes() {
            return attribs;
        }

        @Override
        public Principal getPrincipal() {
            return null;
        }

        @Override
        public boolean isInRole(String role) {
            return false;
        }


    }

}
