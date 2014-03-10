package edu.jhu.pha.vospace.rest;

import java.util.HashMap;
import java.util.Map;

import com.jayway.restassured.authentication.AuthenticationScheme;
import com.jayway.restassured.internal.http.HTTPBuilder;

/**
 * Used for basic and digest authentication
 */
class KeystoneAuth implements AuthenticationScheme {
  private String token = "20645a2fffea4dfab46f3e7568852b9a";
  private static KeystoneAuth inst;

  private KeystoneAuth() {
  }
  
  public static KeystoneAuth getInstance() {
	  if(null == inst)
		  inst = new KeystoneAuth();
	  return inst;
  }
  
  @Override
	public void authenticate(HTTPBuilder httpBuilder) {
	  Map<String, String> headers = (Map<String, String>)httpBuilder.getHeaders();
	  if(null == headers) {
		  headers = new HashMap<String, String>();
		  httpBuilder.setHeaders(headers);
	  }
	  headers.put("X-Auth-Token", token);
	}
}