package edu.jhu.pha.vospace.process.database;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.regex.Matcher;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.codehaus.jackson.JsonNode;

import edu.jhu.pha.vospace.process.sax.AsciiTable;

public class SQLShare implements Database {

	private String userId = "";
	private String apiKey = "";
	private final Logger log = Logger.getLogger(this.getClass());
	
	public SQLShare() {
		
	}
	
	public SQLShare(JsonNode credentials) {
		this.userId = credentials.path("userId").getTextValue();
		this.apiKey = credentials.path("apiKey").getTextValue();
	}
	
	@Override
	public void setup() throws DatabaseException {
		
	}

	@Override
	public void update(Metadata metadata, ArrayList<AsciiTable> dataTables)
			throws DatabaseException {

		for (int i=0; i<dataTables.size(); i++) {
			AsciiTable table = dataTables.get(i); 
			String datasetName = (metadata.get(TikaCoreProperties.SOURCE)).replaceAll("/", Matcher.quoteReplacement("_")) +"_"+table.getTableId();
			System.out.println(datasetName);
			StringBuilder sb = new StringBuilder();
			String[] columnNames = table.getColumnNames();
			for (int j=0; j<columnNames.length; j++) { 
				if (j>0) sb.append(",");
				sb.append(columnNames[j]);
			}
			sb.append("\n");
			ArrayList<String[]> rows = table.getRows();
			for (int j=0; j<rows.size(); j++) {
				String[] row = rows.get(i);
				for (int k=0; k<row.length; k++) {
					if (k>0) sb.append(",");
					sb.append(row[k]);
				}
				sb.append("\n");
			}
			
			try {
				InputStream is = new ByteArrayInputStream(sb.toString().getBytes("US-ASCII"));
				createDataset(is,datasetName);
			}
			catch (Exception e) {
				throw new DatabaseException(e.getMessage());
			}
		}
		
	}

	private void createDataset(InputStream is, String datasetName) throws Exception {
		
	    SSLSocketFactory sf = new SSLSocketFactory(new MyTrustStrategy(), SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

        SchemeRegistry registry = new SchemeRegistry();
        registry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        registry.register(new Scheme("https", 443, sf));
        ClientConnectionManager ccm = new PoolingClientConnectionManager(registry);
        
		HttpClient httpClient = new DefaultHttpClient(ccm);
		
		HttpPost postRequest = new HttpPost("https://sqlshare-rest.cloudapp.net/REST.svc/v3/file");
		postRequest.addHeader("Authorization", "ss_apikey " + userId + " : " + apiKey);
		MultipartEntity multipartEntity = new MultipartEntity();
		InputStreamBody fileBody = new InputStreamBody(is,datasetName); 
		
		multipartEntity.addPart("attachment",fileBody);
		postRequest.setEntity(multipartEntity);
		HttpResponse response = httpClient.execute(postRequest);
		String fileId = EntityUtils.toString(response.getEntity());
		fileId = fileId.replaceAll("\"", "");
        log.debug("file id: "+fileId);
		//System.out.println(fileId);
        
        HttpPut putRequest = new HttpPut("https://sqlshare-rest.cloudapp.net/REST.svc/v3/file/" + fileId + "/database");
        putRequest.addHeader("Authorization", "ss_apikey " + userId + " : " + apiKey);
        httpClient.execute(putRequest);

        if (log.isDebugEnabled()) {
	        int statusCode;
	        do
	        {
	        	Thread.sleep(2000);
	            HttpGet getRequest = new HttpGet("https://sqlshare-rest.cloudapp.net/REST.svc/v3/file/" + fileId + "/database");
	            getRequest.addHeader("Authorization", "ss_apikey " + userId + " : " + apiKey);
	            response = httpClient.execute(getRequest);
	            
	            statusCode = response.getStatusLine().getStatusCode();
	            String s = EntityUtils.toString(response.getEntity());
	            
	            log.debug("status code: "+statusCode);
	            log.debug("response: "+s);
	        }
	        while (statusCode == HttpStatus.SC_ACCEPTED);
        }
	}
	
	@Override
	public void close() throws DatabaseException {}

}
