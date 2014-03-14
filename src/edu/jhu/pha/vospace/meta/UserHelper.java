package edu.jhu.pha.vospace.meta;

import java.io.IOException;
import java.sql.Blob;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import edu.jhu.pha.vospace.api.AccountInfo;
import edu.jhu.pha.vospace.meta.UserGroup;
import edu.jhu.pha.vospace.oauth.SciDriveUser;

public interface UserHelper {

	/** Download a certificate from <tt>certUrl</tt> and save it for the named user in the database.
	 *  If the user doesn't already exist, throw an exception. */
	public abstract void setCertificate(SciDriveUser username, String certUrl)
			throws IOException;

	/** Retrieve a user's certificate as a blob from the database. Null if it doesn't exist.
	 *  If the user doesn't exist, throws an IllegalStateException (so you should call userExists first). */
	public abstract Blob getCertificate(String username);

	public abstract HashMap<String, String> getDataStoreCredentials(
			String username);

	/**
	 * Returns all processors credentials from the DB
	 * @param username
	 * @param processorId
	 * @return
	 */
	public abstract JsonNode getProcessorCredentials(SciDriveUser username);

	public abstract int setDataStoreCredentials(String username,
			String credentials);

	public abstract boolean addDefaultUser(SciDriveUser username);

	public abstract AccountInfo getAccountInfo(SciDriveUser username);

	/** Does the named user exist? */
	public abstract boolean userExists(SciDriveUser username);

	/**
	 * Modifies the user credentials for metadata extractors.
	 * @param username The user ID
	 * @param processorId processor ID (from processors.xml/processor/id)
	 * @param updateNode The processor credentials in JSON format. If null, the processor will be disabled
	 * @return true if success
	 */
	public abstract boolean updateUserService(SciDriveUser username,
			String processorId, JsonNode updateNode);

	public abstract JsonNode getUserServices(SciDriveUser username);

	public abstract List<UserGroup> getGroups(SciDriveUser user);

	public abstract List<String> getGroupUsers(SciDriveUser user, String groupId);
	
	public abstract Share getSharePermission(String userId, String shareId);
}