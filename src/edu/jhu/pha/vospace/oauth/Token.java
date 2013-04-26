package edu.jhu.pha.vospace.oauth;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import javax.ws.rs.core.MultivaluedMap;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.oauth.server.spi.OAuthConsumer;
import com.sun.jersey.oauth.server.spi.OAuthToken;


public class Token implements OAuthToken {
   private final String token;
   private final String secret;
   private final String consumerKey;
   private final String callbackUrl;
   private final Principal principal;
   private final Set<String> roles;
   private final MultivaluedMap<String, String> attribs;

   protected Token(String token, String secret, String consumerKey, String callbackUrl,
           Principal principal, Set<String> roles, MultivaluedMap<String, String> attributes) {
       this.token = token;
       this.secret = secret;
       this.consumerKey = consumerKey;
       this.callbackUrl = callbackUrl;
       this.principal = principal;
       this.roles = roles;
       this.attribs = attributes;
   }

   public Token(String token, String secret, Token requestToken) {
       this(token, secret, requestToken.getConsumer().getKey(), null,
               requestToken.principal, requestToken.roles, new MultivaluedMapImpl());
   }

   public Token(String token, String secret, String consumerKey, String callbackUrl, MultivaluedMap<String, String> attributes) {
       this(token, secret, consumerKey, callbackUrl, null, Collections.<String>emptySet(), attributes);
   }

   @Override
   public String getToken() {
       return token;
   }

   @Override
   public String getSecret() {
       return secret;
   }

   @Override
   public OAuthConsumer getConsumer() {
       return MySQLOAuthProvider2.getConsumer(consumerKey);
   }

   @Override
   public MultivaluedMap<String, String> getAttributes() {
       return attribs;
   }

   @Override
   public Principal getPrincipal() {
       return principal;
   }

   @Override
   public boolean isInRole(String role) {
       return roles.contains(role);
   }

   /** Returns callback URL for this token (applicable just to request tokens)
    *
    * @return callback url
    */
   public String getCallbackUrl() {
       return callbackUrl;
   }

}

