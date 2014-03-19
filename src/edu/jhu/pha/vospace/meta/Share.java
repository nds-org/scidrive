package edu.jhu.pha.vospace.meta;

import java.util.HashMap;

public class Share {

	private String container;
	private String userId;
	private SharePermission permission;
	private HashMap<String, String> storageCredentials;
	
	public Share(String userId, String container, SharePermission permission, HashMap storageCredentials) {
		this.userId = userId;
		this.container = container;
		this.permission = permission;
		this.storageCredentials = storageCredentials;
	}
	
	public HashMap<String, String> getStorageCredentials() {
		return storageCredentials;
	}

	public static enum SharePermission {
		DENIED(false), RW_USER(true), RO_USER(false);
		private boolean canWrite;
		private SharePermission(boolean canWrite) {
			this.canWrite = canWrite;
		}
		public boolean canWrite() {
			return this.canWrite;
		}
	}

	public String getContainer() {
		return container;
	}

	public SharePermission getPermission() {
		return permission;
	}

	public String getUserId() {
		return userId;
	}
}
