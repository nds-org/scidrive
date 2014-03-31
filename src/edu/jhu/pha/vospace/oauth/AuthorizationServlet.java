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
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.glassfish.jersey.server.oauth1.OAuth1Token;
import org.openid4java.OpenIDException;
import org.openid4java.association.AssociationSessionType;
import org.openid4java.consumer.ConsumerAssociationStore;
import org.openid4java.consumer.ConsumerManager;
import org.openid4java.consumer.NonceVerifier;
import org.openid4java.consumer.VerificationResult;
import org.openid4java.discovery.DiscoveryException;
import org.openid4java.discovery.DiscoveryInformation;
import org.openid4java.message.AuthRequest;
import org.openid4java.message.ParameterList;
import org.openid4java.message.ax.FetchRequest;

import edu.jhu.pha.vospace.BaseServlet;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.PermissionDeniedException;
import edu.jhu.pha.vospace.meta.MetaStoreFactory;

/** A simple implementation of an OpenID relying party, specialized for VOSpace & VAO OpenID.
 *  For more sample code, see OpenID4Java's sample code or the USVAO SSO example
 *  (TODO: get URL once it's in usvao svn). */
public class AuthorizationServlet extends BaseServlet {
	private static final long serialVersionUID = -1847330043709412488L;

	private static final Logger logger = Logger.getLogger(AuthorizationServlet.class);

    private static final String ALIAS_CERTIFICATE = "certificate",
            AX_URL_CERTIFICATE = "http://sso.usvao.org/schema/credential/x509";
    
	private static Configuration conf = SettingsServlet.getConfig();

	public static ConsumerAssociationStore assocStore = new OpenidConsumerAssociationStore();
	public static NonceVerifier nonceVer = new OpenidNonceVerifier(5000);

    @Override
    /** Handle GET & POST the same way, because OpenID response may be a URL redirection (GET)
     *  or a form submission (POST). */
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        handle(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        handle(req, resp);
    }

    @Override public String getErrorPage() { return "index.jsp"; }

    private void handle(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException
    {
        logger.debug("Handling request for \"" + request.getRequestURL() + "\"");
        try {
        	
        	List<String> userLogins = null;
        	if(isShareRequest(request)){
        		userLogins = MySQLOAuthProvider2.getShareUsers(request.getParameter("share"));
        		if(null == userLogins || userLogins.isEmpty()) { // open to any user TODO check NULL user
        			authorizeRequestToken(request, response, null);
        			return;
        		}
        		// returns not found exception if not found share
        	}
        	
            if (isOpenIdResponse(request)) {
            	logger.debug("Handle OpenID");
                handleOpenidResponse(request, response);
            } else { // initial login
            	logger.debug("Initiate");
        		logger.debug("OpenID init");
                String provider = request.getParameter("provider");
                String idLess = getIdentityless(provider);
                
                // set cookie for cases when user came directly to authorize from 3rd party application
                if(null != request.getParameter("oauth_token")){
                	OauthCookie cookie = new OauthCookie();
                	cookie.setRequestToken(request.getParameter("oauth_token"));
                	cookie.setCallbackUrl(request.getParameter("oauth_callback"));
                	cookie.setRegion(conf.getString("region"));
                	cookie.setShareId(request.getParameter("share"));
                	response.addCookie(new Cookie(OauthCookie.COOKIE_NAME, cookie.toString()));
                	logger.debug("Created third party app cookie.");
                }
                
                response.sendRedirect(initiateOpenid(request.getRequestURI().toString(), idLess));
            } 
        }
        // for local error-reporting, use a private Exception class, Oops (see below)
        catch(OpenIDException e) {
            handleError(request, response, e.getMessage());
        } catch (Oops e) {
            handleError(request, response, e.getMessage());
		}
    }
    
    private void handleOpenidResponse(HttpServletRequest request, HttpServletResponse response)
            throws IOException, Oops {
        ConsumerManager manager = new ConsumerManager();
        manager.setAssociations(assocStore); 
        manager.setNonceVerifier(nonceVer); 
        manager.setMinAssocSessEnc(AssociationSessionType.DH_SHA256);
        ParameterList params = new ParameterList(request.getParameterMap());
        try {
            VerificationResult verification = manager.verify(request.getRequestURL().toString(), params, null);
            if (null == verification.getVerifiedId() || !isBlank(verification.getStatusMsg()))
                throw new Oops("OpenID authentication failed. " + ((null != verification.getStatusMsg())?verification.getStatusMsg():""));
            // We're authenticated!  Now approve the request token.
            handleAuthenticated(verification, request, response);
        } catch (OpenIDException e) {
            logger.info("Exception verifying OpenID response.", e);
            throw new Oops("Unable to verify OpenID response: " + e.getMessage());
        }
    }

    /** OpenID authentication succeeded. */
    private void handleAuthenticated
            (VerificationResult verification, HttpServletRequest request, HttpServletResponse response)
            throws IOException, Oops {

        // the user's OpenID
        String id = verification.getVerifiedId().getIdentifier();
        logger.debug("Got OpenID "+id);

        // Is the user known to us?
        //String username = getUsername(id);
	        
        // OpenID attribute exchange -- retrieve certificate
        // !!!!!!!!!! Uncomment to retrieve the user's certificate
        
        /*try {
            MessageExtension ext = verification.getAuthResponse().getExtension(AxMessage.OPENID_NS_AX);
            if (ext != null) {
                if (!(ext instanceof FetchResponse))
                    throw new Oops("Unexpected attribute exchange response: " + ext.getClass());
                FetchResponse fetch = (FetchResponse) ext;
                // store credential, if it was returned
                String certUrl = fetch.getAttributeValue(ALIAS_CERTIFICATE);
                if (certUrl != null && !certUrl.isEmpty()) {
	                logger.debug("For user \"" + username + "\" storing cert \"" + certUrl + "\".");
	                UserHelper.setCertificate(username, certUrl);
                }
            }
        } catch (MessageException e) { // we don't expect this to happen
            logger.warn(e);
            throw new Oops("Unable to fetch OpenID Attributes: " + e.getMessage());
        }*/

        // TODO: handle case where access token is already present
        authorizeRequestToken(request, response, SciDriveUser.fromBoundName(id));
    }

	/**
	 * @param request
	 * @param response
	 * @param callbackUrl
	 * @throws IOException
	 * @throws Oops
	 */
	private void authorizeRequestToken(HttpServletRequest request, HttpServletResponse response, SciDriveUser username)
			throws Oops {

        String token = null, callbackUrl = null;
        
        Cookie[] cookies = request.getCookies();
        
        String shareId = null;
        
        if (null != request.getParameter("oauth_token")) {
        	token = request.getParameter("oauth_token");
        	callbackUrl = request.getParameter("oauth_callback");
        } else if(cookies != null) {
        	OauthCookie parsedCookie = null;
        	
        	for (Cookie cookie : cookies) {
	            if (cookie.getName().equals(OauthCookie.COOKIE_NAME)){
	            	// Remove the temporary 3rd party app cookie
	            	Cookie removeCookie = new Cookie(OauthCookie.COOKIE_NAME, "");
	            	removeCookie.setMaxAge(0);
	            	response.addCookie(removeCookie);
	            	try {
	            		parsedCookie = OauthCookie.create(cookie);
	            		shareId = parsedCookie.getShareId();
		    	        if (isBlank(parsedCookie.getRequestToken()))
		    	            throw new Oops("No request token present in oauth cookie (\"" + cookie.getValue() + "\").");
		    	        logger.debug("Parsed oauth cookie \"" + cookie.getValue() + "\" as \"" + parsedCookie.toString() + "\".");
					} catch (IOException e) {
	            		logger.debug("Error parsing cookie. Just removing it.");
					}
	            }
        	}
        	
        	if(null != parsedCookie) {
    	        token = parsedCookie.getRequestToken();
    	        callbackUrl = parsedCookie.getCallbackUrl();
        	}
        }

        if(null == token)
            throw new Oops("No request token found in request.");
		
		try {
			OAuth1Token reqToken = MySQLOAuthProvider2.getRequestToken(token);
			if(null == reqToken)
    			throw new PermissionDeniedException("401 Unauthorized");
            if(null != reqToken.getAttributes().getFirst("root_container")){ // pre-shared container accessor
            	if(shareId != null) {//already created the share - user bound sharing
	        		List<String> groupUserLogins = MySQLOAuthProvider2.getShareUsers(shareId);
	        		if(!groupUserLogins.contains(username)){ // the username of the one authorized != user that share was created for
	        			throw new PermissionDeniedException("401 Unauthorized");
	        		}
            	} // else share is open for everyone
            }
            
            MySQLOAuthProvider2.markAsAuthorized(reqToken, username);

            if(null != callbackUrl && !callbackUrl.isEmpty()){
            	if(callbackUrl.indexOf('?')<=0)
            		callbackUrl += "?"+"oauth_token="+reqToken.getToken();
            	else
            		callbackUrl += "&"+"oauth_token="+reqToken.getToken();
            	logger.debug("Redirecting user to "+callbackUrl);
            	response.sendRedirect(callbackUrl);
            } else {
                response.setContentType("text/plain");
                PrintWriter out = response.getWriter();
                out.println("You have successfully authorized " 
                        + ".\nPlease close this browser window and click continue"
                        + " in the client.");
                out.close();
            }
        } catch (IOException e) {
        	logger.error("Error performing the token authorization "+e.getMessage());
			e.printStackTrace();
            throw new Oops(e.getMessage());
		}
	}

    /** Initiate OpenID authentication.  Return null if successful and no further action is necessary;
     *  return an error message if there was a problem. 
     * @throws Oops 
     * @throws OpenIDException */
    public static String initiateOpenid(String returnUrl, String idLess)
            throws OpenIDException
    {
        ConsumerManager manager = new ConsumerManager();
        manager.setAssociations(assocStore); 
        manager.setNonceVerifier(nonceVer); 
        manager.setMinAssocSessEnc(AssociationSessionType.DH_SHA256);
        try {
            List discoveries = manager.discover(idLess);
            DiscoveryInformation discovered = manager.associate(discoveries);
//            if (returnUrl.indexOf('?') > 0)
//                returnUrl = returnUrl.substring(0, returnUrl.indexOf('?'));
            AuthRequest authRequest = manager.authenticate(discovered, returnUrl);

            // attribute request: get Certificate (could also get name)
//            FetchRequest fetch = FetchRequest.createFetchRequest();
//            fetch.addAttribute(ALIAS_CERTIFICATE, AX_URL_CERTIFICATE, true);
//            authRequest.addExtension(fetch);

            return authRequest.getDestinationUrl(true);
        } catch (DiscoveryException e) {
            logger.warn("Exception during OpenID discovery.", e);
            throw new OpenIDException("Unable to contact OpenID provider: " + e.getMessage());
        } catch (OpenIDException e) {
            logger.warn("Exception processing authentication request.", e);
            throw new OpenIDException("Exception processing authentication request."+ e.getMessage());
        }
    }

    /** The URL to use for identityless authentication for a provider.  Not all providers support it
     * -- we will need to do something fancier with discovery etc. for the general case, although
     * this will work fine with VAO SSO. */
    public static String getIdentityless(String providerName) {
        if (isBlank(providerName))
            throw new IllegalArgumentException("No provider specified.  Try provider=vao.");
        if(null != conf.getString(providerName+".identityless.url"))
            return conf.getString(providerName+".identityless.url");
        else throw new IllegalArgumentException("Unknown provider: \"" + providerName + "\".");
    }

    
    /** Private exception class for displaying error conditions to the user within this servlet. */
    private static class Oops extends Exception {
        Oops(String message) {
            super(message);
            if (message == null)
                throw new NullPointerException("Message is null.");
        }
    }

    private boolean isShareRequest(HttpServletRequest request) {
        return !isBlank(request.getParameter("share"));
    }
    
    private boolean isOpenIdResponse(HttpServletRequest request) {
        return !isBlank(request.getParameter("openid.ns"));
    }
}
