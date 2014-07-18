package edu.jhu.pha.cas.services;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class CasRestClient {
	private String uri;
	
	public CasRestClient(String uri) {
		this.uri = uri;
	}
	
	public String executeQuickQuery(String authToken, String keystoneUserId, String query) throws Exception {
		HttpClient client = new HttpClient();
		PostMethod method = new PostMethod(uri+"/contexts/mydb/query");
		
		method.addRequestHeader("X-Auth-Token", authToken);
		method.addRequestHeader("X-Owner", keystoneUserId);
		method.addRequestHeader("Content-Type","application/json");
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		node.put("Query",query);
		String json = mapper.writeValueAsString(node);
		byte[] content = json.getBytes();
		method.setRequestEntity(new ByteArrayRequestEntity(content));
		try {
			int statusCode = client.executeMethod(method);
			if (statusCode != 200) {
				throw new Exception("HTTP ERROR: "+statusCode);
			}
			byte[] responseBody = method.getResponseBody();
			return new String(responseBody);
    	} 
		finally {
    		method.releaseConnection();
      }  
	}
}
