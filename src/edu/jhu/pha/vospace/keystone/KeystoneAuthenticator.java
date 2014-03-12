package edu.jhu.pha.vospace.keystone;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.rackspacecloud.client.cloudfiles.FilesResponse;

import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.jobs.MyHttpConnectionPoolProvider;

public class KeystoneAuthenticator {
	private final static Configuration conf = SettingsServlet.getConfig();
	private static HttpEntity authEntity= null;
	private static String authToken; 
	private static Logger logger = Logger.getLogger(KeystoneAuthenticator.class);

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	private static final Lock loginLock = new ReentrantLock();
	
	private static ScheduledFuture<JsonNode> loginFuture;
	private static final DateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	static {
		dateForm.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	
	public static String getAdminToken() {
		if(null == authToken)
			login(0);
		return authToken;
	}

	/**
	 * Login for other classes when the token has expired.
	 * If login is already in call, just waits for success, otherwise assigns the token to static var.
	 */
	public static void login(long delay) {
		logger.debug("Login "+delay);
		try {
			if(loginLock.tryLock()) {
				try {
					if(loginFuture != null) { // cancel task if already scheduled, f.e. token was revoked but scheduled login exists
						loginFuture.cancel(true);
					}
						
					loginFuture = scheduler.schedule(new TokenUpdater(), (delay<0)?0:delay, SECONDS);
					JsonNode tokenNode = loginFuture.get();
					
		            String expiresStr = tokenNode.path("access").path("token").path("expires").getTextValue();
		            Date tokenExpDate = dateForm.parse(expiresStr);
		            long expireDelay = TimeUnit.MILLISECONDS.toSeconds(tokenExpDate.getTime() - Calendar.getInstance().getTimeInMillis());
	
					loginFuture = scheduler.schedule(new TokenUpdater(), (expireDelay < 0)?0:expireDelay-16, SECONDS); // 16 sec just in case..
		            
		            authToken = tokenNode.path("access").path("token").path("id").getTextValue();
				} finally {
		            loginLock.unlock();
				}
			} else { // already logging in
				try {
					loginLock.lock();
				} finally {
					loginLock.unlock();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
    
    private static class TokenUpdater implements Callable<JsonNode> {
    	
        @Override
		public JsonNode call() {
    		logger.debug("TokenUpdater called");
            HttpPost method = new HttpPost(conf.getString("keystone.url")+"/v2.0/tokens");
            method.setHeader("Content-Type", "application/json");

    		try {
    			authEntity = new StringEntity("{\"auth\":{\"tenantName\": \""+conf.getString("service.tenant")+"\", \"passwordCredentials\":{\"username\": \""+conf.getString("service.user")+"\",\"password\": \""+conf.getString("service.password")+"\"}}}", "UTF-8");
    		
    	        method.setEntity(authEntity);
    	        
    	        FilesResponse response = new FilesResponse(MyHttpConnectionPoolProvider.getHttpClient().execute(method));
    	        
    	        if (response.loginSuccess()) {
    	            JsonNode tokenNode = new ObjectMapper().readValue(response.getResponseBodyAsStream(), JsonNode.class);
    	        	return tokenNode;
    	        }
    		} catch (UnsupportedEncodingException e) {
    			// Shouldn't happen ever
    		} catch (ClientProtocolException e) {
    			logger.error("Error querying new SciDrive token: "+e.getMessage());
    			e.printStackTrace();
    		} catch (IOException e) {
    			logger.error("Error querying new SciDrive token: "+e.getMessage());
    			e.printStackTrace();
    		}
    		return null;
        }
    }

}
