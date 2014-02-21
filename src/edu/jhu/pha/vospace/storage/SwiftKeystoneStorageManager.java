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
package edu.jhu.pha.vospace.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.client.HttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.type.TypeReference;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.DeserializationConfig;
import com.rackspacecloud.client.cloudfiles.FilesAccountInfo;
import com.rackspacecloud.client.cloudfiles.FilesAuthorizationException;
import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesConstants;
import com.rackspacecloud.client.cloudfiles.FilesContainerExistsException;
import com.rackspacecloud.client.cloudfiles.FilesContainerInfo;
import com.rackspacecloud.client.cloudfiles.FilesContainerNotEmptyException;
import com.rackspacecloud.client.cloudfiles.FilesException;
import com.rackspacecloud.client.cloudfiles.FilesInvalidNameException;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import com.rackspacecloud.client.cloudfiles.FilesResponse;
import edu.jhu.pha.vospace.jobs.MyHttpConnectionPoolProvider;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.InputStreamEntity;


public class SwiftKeystoneStorageManager{
	
	private static int connectionTimeOut;
	private static	String getFileUrl = "http://zinc26.pha.jhu.edu:8081/v1/AUTH_24b79d0aadf04c9eb19dd9aeb5706caa";
	private static String strToken = "d70aa941af764364865d043d0fb725e8";
	private static boolean useETag = true;
	private static HttpClient client = MyHttpConnectionPoolProvider.getHttpClient();
	public static Logger logger = Logger.getLogger(FilesClient.class);


	public static void main(String[] args) throws Exception {		
		//Container Info
		//List<FilesContainerInfo> Containers = listContainersInfo(-1,"E","X");
		//System.out.println(Containers);
		
		
		//Create Container
		//createContainer("XYZ");
		
		//Account Info
		/*FilesAccountInfo f = getAccountInfo();
		System.out.println(f.getBytesUsed());
		System.out.println(f.getContainerCount());*/
		
		//Container Info
		/*FilesContainerInfo f = getContainerInfo("first_cont");
		System.out.println(f.getName());
		System.out.println(f.getObjectCount());
		System.out.println(f.getTotalSize());
		System.out.println(f.getSyncTo());*/
		
		//Delete Container
		/*if(deleteContainer("Test2")){
			System.out.println("deleted");
		}*/
		
		//Update Account Metadata
		/*Map<String, String> map = new HashMap<String, String>();
		map.put("Creator", "Dima");

		if(updateAccountMetadata(map)){
			System.out.println("updated");
		}*/
		
		//Delete Account Metadata
		/*Map<String, String> map = new HashMap<String, String>();
		map.put("Creator", "");

		if(updateAccountMetadata(map)){
			System.out.println("updated");
		}*/
		
		
		//Update Container Metadata
		/*Map<String, String> map = new HashMap<String, String>();
		map.put("Creator", "Dima");
		if(updateContainerMetadata("first_cont",map)){
			System.out.println("updated!");
		}*/
		
		//Delete Container Metadata
				/*Map<String, String> map = new HashMap<String, String>();
				map.put("Creator", "");
				if(updateContainerMetadata("first_cont",map)){
					System.out.println("updated!");
				}*/
		
		//List Objects
		//List<FilesObject> Objects = listObjectsStartingWith("first_cont",null,null,-1,null,null,null);
		//System.out.println(Objects.get(0).getName());
		
		
		//Get Object Data
		/*byte[] obj = getObject("XYZ","Testobj2");
		String s = new String(obj);
		System.out.println(s);*/
		
		//Store Object
		/*Map<String, String> map = new HashMap<String, String>();
		map.put("Creator", "Shradha");
		File f = new File("C:\\Users\\pinkyanup\\Desktop\\Test file.docx");
		InputStream b = new FileInputStream(f);
		String tag = storeStreamedObject("XYZ",b,"text","Testobj1",map);
		System.out.println(tag);*/
		
		//Copy Object
		/*String tag = copyObject("Test1","Testobj","XYZ","Movedobj");
		System.out.println(tag);*/
		
		//Delete Object
		//deleteObject("XYZ","Movedobj");
	}
	
	
	public static List<FilesContainerInfo> listContainersInfo(int limit, String marker, String endMarker) throws IOException, HttpException, FilesAuthorizationException, FilesException
    {
		
    	HttpGet method = null;

    	LinkedList<NameValuePair> parameters = new LinkedList<NameValuePair>();
       		if(limit > 0) {
    			parameters.add(new BasicNameValuePair("limit", String.valueOf(limit)));
    		}
       		if(marker != null) {
    			parameters.add(new BasicNameValuePair("marker", marker));
    		}
       		if(endMarker != null){
       			parameters.add(new BasicNameValuePair("end_marker", endMarker));
       		}
       		parameters.add(new BasicNameValuePair("format", "json"));
       		
    		String uri = makeURI(getFileUrl, parameters);
    		
     		method = new HttpGet(uri);
    		method.addHeader(FilesConstants.X_AUTH_TOKEN, strToken);
    		FilesResponse response = new FilesResponse(client.execute(method));
    		if (response.getStatusCode() == HttpStatus.SC_OK)
    		{

    			ArrayList<FilesContainerInfo> containerList = new ArrayList<FilesContainerInfo>();
       		
       			JsonFactory jsonFactory = new JsonFactory();
       			JsonParser jp = jsonFactory.createJsonParser(IOUtils.toString(response.getResponseBodyAsStream()));
       			ObjectMapper mapper = new ObjectMapper();
       			mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
       			TypeReference ref = new TypeReference<List<FilesContainerInfo>>(){};
       			containerList = mapper.readValue(jp, ref);
       			System.out.println(containerList);
       			return containerList;
       			
    		}		
    		else if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT)
    		{	
    			return new ArrayList<FilesContainerInfo>();
    		}
    		else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND)
    		{
    			throw new FilesNotFoundException("Account not Found", response.getResponseHeaders(), response.getStatusLine());
    		}
    		else {
    			throw new FilesException("Unexpected Return Code", response.getResponseHeaders(), response.getStatusLine());
    		}

    }
	
    public static void createContainer(String name) throws IOException, HttpException, FilesAuthorizationException, FilesException
    {
    		if (isValidContainerName(name))
    		{
    			HttpPut method = new HttpPut(getFileUrl+"/"+sanitizeForURI(name));
    			method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    			method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
     			
    			try {
    				
    				FilesResponse response = new FilesResponse(client.execute(method));    	
    				
 
	    			if (response.getStatusCode() == HttpStatus.SC_CREATED)
	    			{
	    				return;
	    			}
	    			else if (response.getStatusCode() == HttpStatus.SC_ACCEPTED)
	    			{	
	    				throw new FilesContainerExistsException(name, response.getResponseHeaders(), response.getStatusLine());
	    			}
	    			else {
	    				throw new FilesException("Unexpected Response", response.getResponseHeaders(), response.getStatusLine());
	    			}
    			}
    			finally {
    				method.abort();
    			}
    		}
    		else
    		{
    			throw new FilesInvalidNameException(name);
    		}
    	}
    
    public static FilesAccountInfo getAccountInfo() throws IOException, HttpException, FilesAuthorizationException, FilesException
    {

     		HttpHead method = null;
			
     		try {
     			method = new HttpHead(getFileUrl);
     			method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
     			method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
     			FilesResponse response = new FilesResponse(client.execute(method));
 
     			if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT)
     			{
     				int nContainers = response.getAccountContainerCount();
     				long totalSize  = response.getAccountBytesUsed();
     				return new FilesAccountInfo(totalSize,nContainers);
     			}
     			else {
     				throw new FilesException("Unexpected return from server", response.getResponseHeaders(), response.getStatusLine());
     			}
     		}
     		finally {
     			if (method != null) method.abort();
     		}
     	
     }	
    
    public static FilesContainerInfo getContainerInfo (String container) throws IOException, HttpException, FilesException
    {
    	
    		if (isValidContainerName(container))
    		{

    			HttpHead method = null;
    			
    			try {
    				method = new HttpHead(getFileUrl+"/"+sanitizeForURI(container));
    				method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    				method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
    				FilesResponse response = new FilesResponse(client.execute(method));

    				if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT)
    				{
    					int objCount = response.getContainerObjectCount();
    					long objSize  = response.getContainerBytesUsed();
    					String syncTo = response.getContainerSyncTo();
    					return new FilesContainerInfo(container, objCount, objSize, syncTo);
    				}
    				else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND)
    				{
    					throw new FilesNotFoundException("Container not found: " + container, response.getResponseHeaders(), response.getStatusLine());
    				}
    				else {
    					throw new FilesException("Unexpected result from server", response.getResponseHeaders(), response.getStatusLine());
    				}
    			}
    			finally {
    				if (method != null) method.abort();
    			}
    		}
    		else
    		{
    			throw new FilesInvalidNameException(container);
    		}
    	
    }
    
    public static boolean deleteContainer(String name) throws IOException, HttpException, FilesAuthorizationException, FilesInvalidNameException, FilesNotFoundException, FilesContainerNotEmptyException
    {
    	
    		if (isValidContainerName(name))
    		{
    			HttpDelete method = new HttpDelete(getFileUrl+"/"+sanitizeForURI(name));
    			
    			try {
    				method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    	   			method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
        			FilesResponse response = new FilesResponse(client.execute(method));

    	       		if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT)
        			{
        				logger.debug ("Container Deleted : "+name);
        				return true;
        			}
        			else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND)
        			{
        				logger.debug ("Container does not exist !");
           				throw new FilesNotFoundException("You can't delete an non-empty container", response.getResponseHeaders(), response.getStatusLine());
        			}
        			else if (response.getStatusCode() == HttpStatus.SC_CONFLICT)
        			{
        				logger.debug ("Container is not empty, can not delete a none empty container !");
        				throw new FilesContainerNotEmptyException("You can't delete an non-empty container", response.getResponseHeaders(), response.getStatusLine());
        			}
    			}
    			finally {
    				method.abort();
    			}
    		}
    		else
    		{
           		throw new FilesInvalidNameException(name);
    		}
    	
		
    	return false;
    }
    
    public static boolean updateAccountMetadata(Map<String,String> metadata) throws FilesAuthorizationException, 
			HttpException, IOException, FilesInvalidNameException {

	    	HttpPost method = null;
	    	
	    	try {
		    	method = new HttpPost(getFileUrl);
		   		method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
		   		method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
		   		if (!(metadata == null || metadata.isEmpty())) {
		   			for(String key:metadata.keySet())
		   				method.setHeader("X-Account-Meta-"+key, 
		   					FilesClient.sanitizeForURI(metadata.get(key)));
		   		}
		   		FilesResponse response = (FilesResponse) client.execute(method);
	    		return true;
	    	} finally {
	    		if (method != null) 
	    			method.abort();
	    	}
	    	
		}
    
    public static boolean updateContainerMetadata(String name, Map<String,String> metadata) throws FilesAuthorizationException, 
	HttpException, IOException, FilesInvalidNameException {

	FilesResponse response;
	HttpPost method = null;

	try {
    	method = new HttpPost(getFileUrl+"/"+sanitizeForURI(name));
   		method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
   		method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
   		if (!(metadata == null || metadata.isEmpty())) {
   			for(String key:metadata.keySet())
   				method.setHeader("X-Container-Meta-"+key, 
   					FilesClient.sanitizeForURI(metadata.get(key)));
   		}
		HttpResponse resp = client.execute(method);
		response = new FilesResponse(resp);

		
		return true;
	} finally {
		if (method != null) 
			method.abort();
	}
	
}

    public static List<FilesObject> listObjectsStartingWith (String container, String startsWith, String path, int limit, String marker, String end_marker, Character delimiter) throws IOException, FilesException
    {
	
	HttpGet method = null;

		LinkedList<NameValuePair> parameters = new LinkedList<NameValuePair>();
		parameters.add(new BasicNameValuePair ("format", "json"));
		if (startsWith != null) {
			parameters.add(new BasicNameValuePair (FilesConstants.LIST_CONTAINER_NAME_QUERY, startsWith));    		}
   		if(path != null) {
			parameters.add(new BasicNameValuePair("path", path));
		}
   		if(limit > 0) {
			parameters.add(new BasicNameValuePair("limit", String.valueOf(limit)));
		}
   		if(marker != null) {
			parameters.add(new BasicNameValuePair("marker", marker));
		}
   		if(end_marker != null) {
			parameters.add(new BasicNameValuePair("marker", end_marker));
		}
   		if (delimiter != null) {
   			parameters.add(new BasicNameValuePair("delimiter", delimiter.toString()));
   		}
   		
   		String uri = parameters.size() > 0 ? makeURI(getFileUrl+"/"+sanitizeForURI(container), parameters) : getFileUrl;
   		method = new HttpGet(uri);
		method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
		method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
		FilesResponse response = new FilesResponse(client.execute(method));
		
  		if (response.getStatusCode() == HttpStatus.SC_OK)
		{
   			ArrayList <FilesObject> objectList = new ArrayList<FilesObject>();
   			
   			JsonFactory jsonFactory = new JsonFactory();
   			JsonParser jp = jsonFactory.createJsonParser(IOUtils.toString(response.getResponseBodyAsStream()));
   			ObjectMapper mapper = new ObjectMapper();
   			mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
   			TypeReference ref = new TypeReference<List<FilesObject>>(){};
   			objectList = mapper.readValue(jp, ref);
   			return objectList;

			
		}		
		else if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT)
		{	
			logger.debug ("Container "+container+" has no Objects");
			return new ArrayList<FilesObject>();
		}
		else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND)
		{
			throw new FilesNotFoundException("Container was not found", response.getResponseHeaders(), response.getStatusLine());
		}
		else {
			throw new FilesException("Unexpected Server Result", response.getResponseHeaders(), response.getStatusLine());
		}
	}

    public static byte[] getObject (String container, String objName) throws IOException, HttpException, FilesAuthorizationException, FilesInvalidNameException, FilesNotFoundException
    {
    		if (isValidContainerName(container) && FilesClient.isValidObjectName(objName))
    		{
    			HttpGet method = new HttpGet(getFileUrl+"/"+sanitizeForURI(container)+"/"+sanitizeForURI(objName));
    			method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    			method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
     			
    			try {
    				FilesResponse response = new FilesResponse(client.execute(method));

    				if (response.getStatusCode() == HttpStatus.SC_OK)
    				{	
    					logger.debug ("Object data retreived  : "+objName);
    					return response.getResponseBody();
    				}	
    				else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND)
    				{
    					throw new FilesNotFoundException("Container: " + container + " did not have object " + objName, 
									 response.getResponseHeaders(), response.getStatusLine());
    				}
    			}
    			finally {
    				method.abort();
    			}
    		}
    		else
    		{
    			if (!FilesClient.isValidObjectName(objName)) {
    				throw new FilesInvalidNameException(objName);
    			}
    			else {
    				throw new FilesInvalidNameException(container);
    			}
    		}
    	
 
    	return null;
    }
    

    public static String storeStreamedObject(String container, InputStream data, String contentType, String name, Map<String,String> metadata) throws IOException, HttpException, FilesException
    {
 			String objName	 =  name;
			if (isValidContainerName(container) && FilesClient.isValidObjectName(objName))
    		{
				HttpPut method = new HttpPut(getFileUrl+"/"+sanitizeForURI(container)+"/"+sanitizeForURI(objName));

     			method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    			method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
    			InputStreamEntity entity = new InputStreamEntity(data, -1);
    			entity.setChunked(true);
    			entity.setContentType(contentType);
    			method.setEntity(entity);
    			for(String key : metadata.keySet()) {
       				method.setHeader(FilesConstants.X_OBJECT_META + key, sanitizeForURI(metadata.get(key)));
    			}
    			method.removeHeaders("Content-Length");
  
    			
    			try {
        			FilesResponse response = new FilesResponse(client.execute(method));
        			
        			if (response.getStatusCode() == HttpStatus.SC_CREATED)
        			{
        				return response.getResponseHeader(FilesConstants.E_TAG).getValue();
        			}
        			else {
        				logger.error(response.getStatusLine());
        				throw new FilesException("Unexpected result", response.getResponseHeaders(), response.getStatusLine());
        			}
    			}
    			finally {	
    				method.abort();
    			}
    		}
    		else
    		{
    			if (!FilesClient.isValidObjectName(objName)) {
    				throw new FilesInvalidNameException(objName);
    			}
    			else {
    				throw new FilesInvalidNameException(container);
    			}
    		}
    	}
    
    public static String copyObject(String sourceContainer,
            String sourceObjName,
            String destContainer,
            String destObjName) throws HttpException, IOException {
    	
    	String etag = null;

    	if (isValidContainerName(sourceContainer) &&
    			FilesClient.isValidObjectName(sourceObjName) &&
    			isValidContainerName(destContainer) &&
    			FilesClient.isValidObjectName(destObjName)) {

    		HttpPut method = null;
    		
    		try {
    			String sourceURI = sanitizeForURI(sourceContainer) +
    					"/" + sanitizeForURI(sourceObjName);
    			String destinationURI = sanitizeForURI(destContainer) +
    					"/" + sanitizeForURI(destObjName);

    			method = new HttpPut(getFileUrl + "/" + destinationURI);
    			method.getParams().setIntParameter("http.socket.timeout",
                                      connectionTimeOut);
    			method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
    			method.setHeader(FilesConstants.X_COPY_FROM, sourceURI);

    			FilesResponse response = new FilesResponse(client.execute(
    					method));

    			if (response.getStatusCode() == HttpStatus.SC_CREATED) {
    				etag = response.getResponseHeader(FilesConstants.E_TAG)
    						.getValue();

    			} 
    			else {
    				throw new FilesException("Unexpected status from server",
                                response.getResponseHeaders(),
                                response.getStatusLine());
    				}

    		} finally {
    			if (method != null) {
    				method.abort();
    			}
    		}
    	} else {
    		if (!isValidContainerName(sourceContainer)) {
    			throw new FilesInvalidNameException(sourceContainer);
    		} else if (!FilesClient.isValidObjectName(sourceObjName)) {
    			throw new FilesInvalidNameException(sourceObjName);
    		} else if (!isValidContainerName(destContainer)) {
    			throw new FilesInvalidNameException(destContainer);
    		} else {
    			throw new FilesInvalidNameException(destObjName);
    		}
    		}
     
	return etag;
	}
 
    public static void deleteObject (String container, String objName) throws IOException, FilesNotFoundException, HttpException, FilesException
    {

    		if (isValidContainerName(container) && FilesClient.isValidObjectName(objName))
    		{
    			HttpDelete method = null;
    			
    			try {
    				method = new HttpDelete(getFileUrl+"/"+sanitizeForURI(container)+"/"+sanitizeForURI(objName));
    				method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    				method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
    				FilesResponse response = new FilesResponse(client.execute(method));
    				
           			if (response.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
           				method.abort();
           				method = new HttpDelete(getFileUrl+"/"+sanitizeForURI(container)+"/"+sanitizeForURI(objName));
            			method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
        				method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
        				method.setHeader(FilesConstants.X_AUTH_TOKEN, strToken);
        				response = new FilesResponse(client.execute(method));
        			}


    				if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT)
    				{
    					logger.debug ("Object Deleted : "+objName);
    				}
    				else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND)
    				{
    					throw new FilesNotFoundException("Object was not found " + objName, response.getResponseHeaders(), response.getStatusLine());
    				}
    				else {
    					throw new FilesException("Unexpected status from server", response.getResponseHeaders(), response.getStatusLine());
    				}
    			}
    			finally {
    				if (method != null) method.abort();
    			}
    		}
    		else
    		{
    			if (!FilesClient.isValidObjectName(objName)) {
    				throw new FilesInvalidNameException(objName);
    			}
    			else {
    				throw new FilesInvalidNameException(container);
    			}
    		}
    	}
 
    
    private static String makeURI(String base, List<NameValuePair> parameters) {
		return base + "?" + URLEncodedUtils.format(parameters, "UTF-8");
	}
	
	public static boolean isValidContainerName(String name) {
		if (name == null) return false;
		int length = name.length();
		if (length == 0 || length > FilesConstants.CONTAINER_NAME_LENGTH) return false;
		if (name.indexOf('/') != -1) return false;
		return true;
	}
	

    public static String sanitizeForURI(String str) {
    	URLCodec codec= new URLCodec();
    	try {
    		return codec.encode(str).replaceAll("\\+", "%20");
    	}
    	catch (EncoderException ee) {
     		return str;
    	}
    }
	
}	
	
     





