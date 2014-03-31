package edu.jhu.pha.vospace.oauth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.openid4java.association.Association;
import org.openid4java.consumer.ConsumerAssociationStore;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;

/**
 * 
 * The specified table must have the following structure:
 * <ul>
 * <li>opurl : string </li>
 * <li>handle : string : primary key</li>
 * <li>type : string</li>
 * <li>mackey : string</li>
 * <li>expdate : date</li>
 * </ul>
 * 
 * @author Dmitry Mishin
 */
public class OpenidConsumerAssociationStore implements ConsumerAssociationStore {

	private static final Logger logger = Logger.getLogger(AuthorizationServlet.class);

	@Override
	public Association load(final String opUrl, final String handle) {
    	return DbPoolServlet.goSql("Get openid association",
    		"SELECT * FROM openid_associations WHERE opurl=? AND handle=?",
            new SqlWorker<Association>() {
                @Override
                public Association go(Connection conn, PreparedStatement stmt) throws SQLException {
                	stmt.setString(1, opUrl);
                	stmt.setString(2, handle);
                	ResultSet resSet = stmt.executeQuery();
                	if(resSet.next()) {
            			String type = (String) resSet.getString("type");
            			String macKey = (String) resSet.getString("mackey");
            			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            			cal.setTimeInMillis(resSet.getTimestamp("expdate").getTime());
            			Date expDate =  cal.getTime();

            			if (type == null || macKey == null || expDate == null) {
                			logger.error(
                					"Invalid association data retrived from database; cannot create Association "
    								+ "object for handle: " + handle);
                			return null;
            			}
            			
            			Association assoc;

            			if (Association.TYPE_HMAC_SHA1.equals(type))
            				assoc = Association.createHmacSha1(handle,
            						Base64.decodeBase64(macKey.getBytes()), expDate);

            			else if (Association.TYPE_HMAC_SHA256.equals(type))
            				assoc = Association.createHmacSha256(handle,
            						Base64.decodeBase64(macKey.getBytes()), expDate);

            			else {
                			logger.error("Invalid association type "
            						+ "retrieved from database: " + type);
                			return null;
            			}

            			return assoc;
                	} else {
            			logger.error("Association not found for handle: " + handle);
            			return null;
                	}
                }
            }
        );
	}

	@Override
	public Association load(final String opUrl) {

    	return DbPoolServlet.goSql("Get openid association",
    			"SELECT * FROM openid_associations"
				+ " T1 JOIN (SELECT opurl, max(expdate) AS expdate FROM openid_associations"
				+ " WHERE opurl=? GROUP BY opurl) T2 ON (T1.expdate = T2.expdate AND T1.opurl = T2.opurl)",
                new SqlWorker<Association>() {
                    @Override
                    public Association go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	stmt.setString(1, opUrl);
                    	ResultSet resSet = stmt.executeQuery();
                    	if(resSet.next()) {
                			String handle = (String) resSet.getString("handle");
                			String type = (String) resSet.getString("type");
                			String macKey = (String) resSet.getString("mackey");
                			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                			cal.setTimeInMillis(resSet.getTimestamp("expdate").getTime());
                			Date expDate =  cal.getTime();

                			Association assoc;

                			if (type == null || macKey == null || expDate == null
                					&& !Association.FAILED_ASSOC_HANDLE.equals(handle)) {
                    			logger.error(
                						"Invalid expiry date retrived from database; cannot create Association "
                								+ "object for handle: " + handle);
                    			return null;
                			} else if (Association.FAILED_ASSOC_HANDLE.equals(handle)) {
                				assoc = Association.getFailedAssociation(expDate);

                			} else if (Association.TYPE_HMAC_SHA1.equals(type)) {
                				assoc = Association.createHmacSha1(handle,
                						Base64.decodeBase64(macKey.getBytes()), expDate);

                			} else if (Association.TYPE_HMAC_SHA256.equals(type)) {
                				assoc = Association.createHmacSha256(handle,
                						Base64.decodeBase64(macKey.getBytes()), expDate);

                			} else {
                    			logger.error(
                    					"Invalid association type "
                						+ "retrieved from database: " + type);
                    			return null;
                			}
                			
                			return assoc;
                    	} else {
                			logger.error("Association not found for opUrl: " + opUrl);
                			return null;
                    	}
                    }
                }
            );
	}

	@Override
	public void remove(final String opUrl, final String handle) {
    	DbPoolServlet.goSql("Remove openid association",
			"DELETE FROM openid_associations "
			+ " WHERE opurl=? AND handle=?",
            new SqlWorker<Boolean>() {
                @Override
                public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                	stmt.setString(1, opUrl);
                	stmt.setString(2, handle);
                	return stmt.execute();
                }
    		}
		);
	}

	@Override
	public void save(final String opUrl, final Association association) {
		cleanupExpired();

    	DbPoolServlet.goSql("Save openid association",
			"INSERT INTO openid_associations VALUES (?,?,?,?,?)",
            new SqlWorker<Boolean>() {
                @Override
                public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                	stmt.setString(1, opUrl);
                	stmt.setString(2, association.getHandle());
                	stmt.setString(3, association.getType());
                	stmt.setString(4, association.getMacKey() == null ? null
							: new String(Base64
									.encodeBase64(association
											.getMacKey()
											.getEncoded())));
                	stmt.setTimestamp(5, new Timestamp(association.getExpiry().getTime()));
                	return stmt.execute();
                }
    		}
		);
	}


	private static void cleanupExpired() {
		DbPoolServlet.goSql("Cleanup openid association",
			"DELETE FROM openid_associations WHERE expdate < NOW()",
	        new SqlWorker<Boolean>() {
	            @Override
	            public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
	            	return stmt.execute();
	            }
			}
		);
	}

}
