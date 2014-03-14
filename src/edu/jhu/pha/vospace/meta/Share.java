package edu.jhu.pha.vospace.meta;

public class Share {

	private String container;
	private String userId;
	private SharePermission permission;
	
	public Share(String userId, String container, SharePermission permission) {
		this.userId = userId;
		this.container = container;
		this.permission = permission;
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
