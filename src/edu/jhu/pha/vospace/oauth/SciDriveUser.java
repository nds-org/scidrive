package edu.jhu.pha.vospace.oauth;

import java.security.Principal;
import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.meta.MetaStoreFactory;

public class SciDriveUser implements Principal {

	private String name;
	private String rootContainer;
	private boolean writeEnabled;
	private HashMap<String, String> storageCredentials;
	
	private static final Logger logger = Logger.getLogger(SciDriveUser.class);
	
	public SciDriveUser(String name, String rootContainer, boolean writeEnabled, HashMap<String, String> storageCredentials) {
		this.name = name;
		this.rootContainer = rootContainer;
		this.writeEnabled = writeEnabled;
		this.storageCredentials = storageCredentials;
	}

	public static SciDriveUser fromName(String name) {
		return new SciDriveUser(name, "", true, MetaStoreFactory.getUserHelper().getDataStoreCredentials(name));
	}
	
	@Override
	public String getName() {
		return name;
	}

	public String getRootContainer() {
		return rootContainer;
	}

	public boolean isWriteEnabled() {
		return writeEnabled;
	}

	public String getAccountUrl() {
		return this.storageCredentials.get("storageurl");
	}

}
