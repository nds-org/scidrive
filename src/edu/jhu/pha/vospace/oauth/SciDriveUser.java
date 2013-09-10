package edu.jhu.pha.vospace.oauth;

import java.security.Principal;

public class SciDriveUser implements Principal {

	private String name;
	private String rootContainer;
	private boolean writeEnabled;

	public SciDriveUser(String name, String rootContainer, boolean writeEnabled) {
		this.name = name;
		this.rootContainer = rootContainer;
		this.writeEnabled = writeEnabled;
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

}
