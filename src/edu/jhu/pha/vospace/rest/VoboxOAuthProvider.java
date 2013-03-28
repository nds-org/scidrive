package edu.jhu.pha.vospace.rest;

import java.util.List;
import java.util.Map;

import javax.ws.rs.ext.Provider;

import com.sun.jersey.oauth.server.spi.OAuthConsumer;
import com.sun.jersey.oauth.server.spi.OAuthProvider;
import com.sun.jersey.oauth.server.spi.OAuthToken;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import edu.jhu.pha.vospace.oauth.MySQLOAuthProvider2;
import edu.jhu.pha.vospace.oauth.Token;

import javax.ws.rs.core.MultivaluedMap;

@Provider
public class VoboxOAuthProvider implements OAuthProvider {

    @Override
    public OAuthConsumer getConsumer(String consumerKey) {
    	return MySQLOAuthProvider2.getConsumer(consumerKey);
    }

    @Override
    public Token getRequestToken(String token) {
        return MySQLOAuthProvider2.getRequestToken(token);
    }

    @Override
    public OAuthToken newRequestToken(String consumerKey, String callbackUrl, Map<String, List<String>> attributes) {
        return MySQLOAuthProvider2.generateRequestToken(consumerKey, callbackUrl, mapToMulti(attributes));
    }

    private static MultivaluedMap<String, String> mapToMulti(Map<String, List<String>> map) {
    	MultivaluedMap<String, String> newMap = new MultivaluedMapImpl();
    	for(String key: map.keySet()) {
    		List<String> values = map.get(key);
    		for(String value: values) {
    			newMap.add(key, value);
    		}
    	}
    	return newMap;
    }
    
    @Override
    public OAuthToken newAccessToken(OAuthToken requestToken, String verifier) {
    	return MySQLOAuthProvider2.generateAccessToken((Token)requestToken, verifier);
    }

    @Override
    public OAuthToken getAccessToken(String token) {
        return MySQLOAuthProvider2.getAccessToken(token);
    }

    public static class Consumer implements OAuthConsumer {
        private final String key;
        private final String secret;
        private final String owner;
        private final MultivaluedMap<String, String> attribs;

        public Consumer(String key, String secret, String owner, MultivaluedMap<String, String> attributes) {
            this.key = key;
            this.secret = secret;
            this.owner = owner;
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

        /** Returns identifier of owner of this consumer - i.e. who registered
         * the consumer.
         *
         * @return consumer owner
         */
        public String getOwner() {
            return owner;
        }

        /** Returns additional attributes associated with the consumer (e.g. name,
         * URI, description, etc.)
         *
         * @return name-values pairs of additional attributes
         */
        public MultivaluedMap<String, String> getAttributes() {
            return attribs;
        }

    }

}
