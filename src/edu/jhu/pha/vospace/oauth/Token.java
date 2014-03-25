package edu.jhu.pha.vospace.oauth;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import javax.ws.rs.core.MultivaluedMap;

import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;
import org.glassfish.jersey.server.oauth1.OAuth1Consumer;
import org.glassfish.jersey.server.oauth1.OAuth1Token;



public class Token implements OAuth1Token {
   private final String token;
   private final String secret;
   private final String consumerKey;
   private final String callbackUrl;
   private final Principal principal;
   private final Set<USER_ROLES> roles;
   private final MultivaluedMap<String, String> attribs;
   public enum USER_ROLES {user, rwshareuser, roshareuser};

   protected Token(String token, String secret, String consumerKey, String callbackUrl,
           Principal principal, Set<USER_ROLES> roles, MultivaluedMap<String, String> attributes) {
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
               requestToken.principal, requestToken.roles, new MultivaluedStringMap());
   }

   public Token(String token, String secret, String consumerKey, String callbackUrl, MultivaluedMap<String, String> attributes) {
       this(token, secret, consumerKey, callbackUrl, null, Collections.<USER_ROLES>emptySet(), attributes);
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
   public OAuth1Consumer getConsumer() {
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
   public boolean isInRole(String roleStr) {
	   try {
		   USER_ROLES role = Enum.valueOf(USER_ROLES.class, roleStr);
		   return roles.contains(role);
	   } catch (Exception ex) {
		   return false;
	   }
   }

   /** Returns callback URL for this token (applicable just to request tokens)
    *
    * @return callback url
    */
   public String getCallbackUrl() {
       return callbackUrl;
   }

}

