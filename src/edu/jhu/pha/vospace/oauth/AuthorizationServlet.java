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
import java.io.File;
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
import org.apache.log4j.Level;
import org.openid4java.OpenIDException;
import org.openid4java.consumer.ConsumerManager;
import org.openid4java.consumer.VerificationResult;
import org.openid4java.discovery.DiscoveryException;
import org.openid4java.discovery.DiscoveryInformation;
import org.openid4java.message.AuthRequest;
import org.openid4java.message.ParameterList;
import org.openid4java.message.ax.FetchRequest;

import edu.jhu.pha.vospace.BaseServlet;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.PermissionDeniedException;

import edu.uiuc.ncsa.myproxy.oa4mp.client.Asset;
import edu.uiuc.ncsa.myproxy.oa4mp.client.OA4MPResponse;
import edu.uiuc.ncsa.myproxy.oa4mp.client.servlet.ClientServlet;
import edu.uiuc.ncsa.myproxy.oa4mp.client.servlet.sample.SimpleStartRequest;
import edu.uiuc.ncsa.myproxy.oa4mp.client.servlet.sample.SimpleReadyServlet;
import edu.uiuc.ncsa.myproxy.oa4mp.client.storage.AssetStore;
import edu.uiuc.ncsa.myproxy.oa4mp.client.storage.AssetStoreUtil;
import edu.uiuc.ncsa.security.core.Identifier;
import edu.uiuc.ncsa.security.servlet.JSPUtil;
import edu.uiuc.ncsa.security.util.pkcs.KeyUtil;

import edu.uiuc.ncsa.myproxy.oa4mp.client.OA4MPService;
import edu.uiuc.ncsa.myproxy.oa4mp.client.ClientEnvironment;
import edu.uiuc.ncsa.myproxy.oa4mp.client.loader.ClientEnvironmentUtil;
import edu.uiuc.ncsa.myproxy.oa4mp.client.AssetResponse;

/** A simple implementation of an OpenID relying party, specialized for VOSpace & VAO OpenID.
 *  For more sample code, see OpenID4Java's sample code or the USVAO SSO example
 *  (TODO: get URL once it's in usvao svn). */
public class AuthorizationServlet extends BaseServlet {
	private static final long serialVersionUID = -1847330043709412488L;

	private static final Logger logger = Logger.getLogger(AuthorizationServlet.class);

    private static final String ALIAS_CERTIFICATE = "certificate",
            AX_URL_CERTIFICATE = "http://sso.usvao.org/schema/credential/x509";
    
	private static Configuration conf = SettingsServlet.getConfig();
    
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
            } else if (isOA4MPResponse(request)) {
            	logger.debug("Handle OA4MP response");
                handleOA4MPResponse(request, response);
            } else { // initial login
            	logger.debug("Initiate");
            	String userName = checkCertificate(request);
            	if(null != userName){ // made X.509 authentication
            		logger.debug("Certificate checked. Username: "+userName);

                    if (!UserHelper.userExists(userName)) {
                        UserHelper.addDefaultUser(userName);
                    }

            		authorizeRequestToken(request, response, userName);
            	} else { // need to do openid
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
	                
	                // String error = initiateOpenid(request, response, idLess);
	                String error = initiateOA4MP(request, response);
	                if (error != null)
	                    throw new Oops(error);
            	}
            } 
        }
        // for local error-reporting, use a private Exception class, Oops (see below)
        catch(Oops e) {
            handleError(request, response, e.getMessage());
        }
    }

    private String checkCertificate(HttpServletRequest request) {
    	java.security.cert.X509Certificate[] certs =
    		(java.security.cert.X509Certificate[]) request.getAttribute(
    				"javax.servlet.request.X509Certificate");

    	if(null != certs){
    		if (certs[0] != null) {
    			String dn = certs[0].getSubjectX500Principal().getName();
    			try {
    				LdapName ldn = new LdapName(dn);
    				Iterator<Rdn> rdns = ldn.getRdns().iterator();
    				String org = null, cn = null;
    				while (rdns.hasNext()) {
    					Rdn rdn = (Rdn) rdns.next();
    					if (rdn.getType().equalsIgnoreCase("O"))
    						org = (String) rdn.getValue();
    					else if (rdn.getType().equalsIgnoreCase("CN"))
    						cn = (String) rdn.getValue();
    				}
    				if (cn != null){
    					return cn;
    				} else {
    					logger.error("Error authenticating the user: cn not found in certificate.");
    					throw new PermissionDeniedException("401 Unauthorized");
    				}
    			} catch (javax.naming.InvalidNameException e) {
    			}
    		}
    	}
    	return null;
    }
    
    private void handleOpenidResponse(HttpServletRequest request, HttpServletResponse response)
            throws IOException, Oops {
        ConsumerManager manager = new ConsumerManager();
        manager.setAllowStateless(false);
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

    private void handleOA4MPResponse(HttpServletRequest request, HttpServletResponse response)
            throws IOException, Oops {
        try {
        logger.debug("IN OA4MP response ");
        ClientEnvironment ce = ClientEnvironmentUtil.load(new File("/etc/vospace/client-cfg.xml"));
        logger.debug("READ FILE ");
        OA4MPService service = new OA4MPService(ce);
        logger.debug("OA4MP SERVICE");
        String accessToken = request.getParameter("oauth_token"); // from  the callback URL
        String verifier = request.getParameter("oauth_verifier");   // from  the callback URL
        logger.debug("OA4MP accessToken " + accessToken + " verifier " + verifier);
        AssetResponse assetResponse = service.getCert(accessToken, verifier);
        logger.debug("OA4MP assetResponse " + assetResponse);
        authorizeRequestToken2(request, response, assetResponse.getUsername());
        } catch (Exception e) {
            logger.info("Exception verifying OA4MP response.", e);
            throw new Oops("Unable to verify OA4MP response: " + e.getMessage());
        }
    }

    /** OpenID authentication succeeded. */
    private void handleAuthenticated
            (VerificationResult verification, HttpServletRequest request, HttpServletResponse response)
            throws IOException, Oops {

        // the user's OpenID
        String id = verification.getVerifiedId().getIdentifier();

        // Is the user known to us?
        //String username = getUsername(id);
        String username = id;
	        
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
        authorizeRequestToken(request, response, username);
    }

	/**
	 * @param request
	 * @param response
	 * @param callbackUrl
	 * @throws IOException
	 * @throws Oops
	 */
	private void authorizeRequestToken(HttpServletRequest request, HttpServletResponse response, String username)
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
			Token reqToken = MySQLOAuthProvider2.getRequestToken(token);
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
	private void authorizeRequestToken2(HttpServletRequest request, HttpServletResponse response, String username)
			throws Oops {

        String token = null, callbackUrl = null;
        
        Cookie[] cookies = request.getCookies();
        
        String shareId = null;
        logger.debug("IN authorizeRequestToken2 " + cookies.toString());
        
        // if (null != request.getParameter("oauth_token")) {
        //  	token = request.getParameter("oauth_token");
        //  	callbackUrl = request.getParameter("oauth_callback");
        // } else if(cookies != null) {
        if(cookies != null) {
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
			Token reqToken = MySQLOAuthProvider2.getRequestToken(token);
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
     *  return an error message if there was a problem. */
    private String initiateOpenid(HttpServletRequest request, HttpServletResponse response, String idLess)
            throws IOException
    {
        ConsumerManager manager = new ConsumerManager();
        manager.setAllowStateless(false);
        try {
            List discoveries = manager.discover(idLess);
            DiscoveryInformation discovered = manager.associate(discoveries);
            String returnUrl = request.getRequestURL().toString();
            if (returnUrl.indexOf('?') > 0)
                returnUrl = returnUrl.substring(0, returnUrl.indexOf('?'));
            AuthRequest authRequest = manager.authenticate(discovered, returnUrl);

            // attribute request: get Certificate (could also get name)
            FetchRequest fetch = FetchRequest.createFetchRequest();
            fetch.addAttribute(ALIAS_CERTIFICATE, AX_URL_CERTIFICATE, true);
            authRequest.addExtension(fetch);

            response.sendRedirect(authRequest.getDestinationUrl(true));
        } catch (DiscoveryException e) {
            logger.warn("Exception during OpenID discovery.", e);
            return "Unable to contact OpenID provider: " + e.getMessage();
        } catch (OpenIDException e) {
            logger.warn("Exception processing authentication request.", e);
        }
        return null; // no errors
    }

    private String initiateOA4MP(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        try {
        logger.debug("BEFORE FILE ");
        ClientEnvironment ce = ClientEnvironmentUtil.load(new File("/etc/vospace/client-cfg.xml"));
        logger.debug("READ FILE ");
        OA4MPService service = new OA4MPService(ce);
        logger.debug("OA4MP SERVICE");
        OA4MPResponse oa4mpResponse = service.requestCert();
        logger.debug("REDIRECTING TO: " + oa4mpResponse.getRedirect().toURL().toString());
        response.sendRedirect(oa4mpResponse.getRedirect().toURL().toString());
        } catch (Exception e) {
            logger.error("Exception during OA4MP initiation.", e);
            return "Unable to form OA4MP request: " + e.getMessage();
        }
        return null;
    }

    /** The URL to use for identityless authentication for a provider.  Not all providers support it
     * -- we will need to do something fancier with discovery etc. for the general case, although
     * this will work fine with VAO SSO. */
    private static String getIdentityless(String providerName) {
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
    
    private boolean isOA4MPResponse(HttpServletRequest request) {
        return !isBlank(request.getParameter("oauth_verifier"));
    }
}
