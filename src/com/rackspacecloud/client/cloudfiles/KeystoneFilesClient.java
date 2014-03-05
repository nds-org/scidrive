/*
 * See COPYING for license information.
 */ 

package com.rackspacecloud.client.cloudfiles;

import java.io.IOException;
import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.client.HttpClient;
import org.apache.log4j.Logger;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.keystone.KeystoneAuthenticator;
import edu.jhu.pha.vospace.oauth.SciDriveUser;

/**
 * @author dimm
 */
public class KeystoneFilesClient extends FilesClient{

	private static HttpEntity authEntity;
	private final static Configuration conf = SettingsServlet.getConfig();


	private static Logger logger = Logger.getLogger(KeystoneFilesClient.class); 

    /**
     * @param client    The HttpClient to talk to Swift
     * @param username  The username to log in to 
     * @param password  The password
     * @param account   The Cloud Files account to use
     * @param connectionTimeOut  The connection timeout, in ms.
     */
    public KeystoneFilesClient(HttpClient client, SciDriveUser username, String password, String authUrl, String account, int connectionTimeOut) {
		super(client, username.getName(), password, authUrl, account, connectionTimeOut);
		this.authenticationURL = conf.getString("keystone.url");
		this.isLoggedin   = true;
		this.storageURL = username.getAccountUrl();
    	this.authToken = KeystoneAuthenticator.getAdminToken();
    	this.isLoggedin = true;
    }
    
    /**
     * Log in to CloudFiles.  This method performs the authentication and sets up the client's internal state.
     * 
     * @return true if the login was successful, false otherwise.
     * 
     * @throws IOException   There was an IO error doing network communication
     * @throws HttpException There was an error with the http protocol
     */
    @Override
	public boolean login() throws IOException, HttpException
    {
    	KeystoneAuthenticator.login(0);
    	this.authToken = KeystoneAuthenticator.getAdminToken();
        return true;
    }
}
