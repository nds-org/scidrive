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
package edu.jhu.pha.vospace.node;

import java.io.UnsupportedEncodingException;
import org.apache.commons.httpclient.URIException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.URI;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.SettingsServlet;

public class VospaceId {
	private static final Pattern VOS_PATTERN = Pattern.compile("vos://[\\w\\d][\\w\\d\\-_\\.!~\\*'\\(\\)\\+=]{2,}![\\w\\d\\-_\\.!~\\*'\\(\\)\\+\\%=/]+");

	private static final String DEFAULT_URI = SettingsServlet.getConfig().getString("region");

	private static final Logger logger = Logger.getLogger(VospaceId.class);
	
	private NodePath nodePath;
	private String uri;
	
	public VospaceId(String idStr) throws URIException {
		URI voURI = new URI(idStr, false, "UTF-8");

		if(!validId(voURI)){
			throw new URIException(idStr);
		}
		
		if(!StringUtils.contains(idStr, "vospace")) {
			throw new URIException(idStr);
		}
		
		this.uri = StringUtils.substringBetween(idStr, "vos://", "!vospace");
		
		if(this.uri == null)
			throw new URIException(idStr);
			
		try {
			String pathStr = URLDecoder.decode(StringUtils.substringAfter(idStr, "!vospace"), "UTF-8");
			this.nodePath = new NodePath(pathStr);
		} catch (UnsupportedEncodingException e) {
			// should not happen
			logger.error(e.getMessage());
		}
		
	}

	/**
	 * Creates new Vospace ID with current URI
	 * @param path Node path
	 * @throws URIException
	 */
	public VospaceId (NodePath path) throws URIException {
		this(path, DEFAULT_URI);
	}
	
	/**
	 * Creates new Vospace ID
	 * @param path Node path
	 * @param uri Node ID uri
	 * @throws URIException
	 */
	private VospaceId (NodePath path, String uri) throws URIException {
		this.nodePath = new NodePath(path.getNodeStoragePath(), path.isEnableAppContainer());
		this.uri = uri;
		this.nodePath.setEnableAppContainer(path.isEnableAppContainer());
	}
	
	public URI getId() {
		try {
			return toUri(nodePath.getNodeStoragePath(), uri);
		} catch(URIException ex) {
			// Should be already checked in the constructor
			logger.error(ex.getMessage());
			return null;
		}
	}
	
	private static URI toUri(String path, String uri) throws URIException {
		return new URI("vos", "//"+uri+"!vospace"+path, null);
	}
	
	/**
	 * Check whether the specified identifier is valid
	 * @param id The identifier to check
	 * @return whether the identifier is valid or not
	 */
	private static boolean validId(URI id) {
		Matcher m = VOS_PATTERN.matcher(id.toString());
		return m.matches();
	}

	/**
	 * Check whether the specified identifier is local
	 * @param id The identifier to check
	 * @return whether the identifier is local or not
	 */
	public boolean isLocalId(String uri) {
		return this.uri.equals(uri);
	}

	public VospaceId appendPath(NodePath path) throws URIException {
		return new VospaceId(this.nodePath.append(path), this.uri);
	}
	
	public NodePath getNodePath() {
		return this.nodePath;
	}
	
	public String toString() {
		return this.getId().toString();
	}
	
	public VospaceId getParent() throws URIException {
		return new VospaceId(this.getNodePath().getParentPath(), this.uri);
	}

	public static final void main(String[] s) throws URIException, ParseException {
		VospaceId[] ids = new VospaceId[]{
				new VospaceId("vos://edu.jhu!vospace/cont3/dat a1"),
				new VospaceId("vos://edu.jhu!vospace/cont3"),
				new VospaceId("vos://edu.jhu!vospace/"),
				new VospaceId("vos://edu.jhu!vospace"),
				new VospaceId(new NodePath("/cont3/data1"),"edu.jhu"),
				new VospaceId(new NodePath("/"),"edu.jhu"),
			};
		
		for(VospaceId id1: ids){
			System.out.println(id1.toString());
			System.out.println(id1.isLocalId("edu.jhu"));
			System.out.println(id1.isLocalId("edu.jhu2"));
			//System.out.println(id1.samePath("/cont3/data1"));
			//System.out.println(id1.samePath("/cont3/data2"));
			//System.out.println(id1.id.getRawAuthority()+" "+id1.id.getPath());
			System.out.println();
		}
		
		//System.out.println(new VospaceId("vos://edu.jhu!vospace/cont3/data1/data4").getRelativePath(new VospaceId("vos://edu.jhu!vospace/cont3")));
	}
	
}
