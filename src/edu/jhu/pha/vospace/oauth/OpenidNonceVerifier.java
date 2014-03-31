package edu.jhu.pha.vospace.oauth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import org.openid4java.consumer.NonceVerifier;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;

/**
 * 
 * JDBC implementation of a NonceVerifier.
 * <p>
 * Since the nonces are constructed on the web server and not on the shared
 * database server, they may accidentally collide.
 * </p>
 * <p>
 * The specified table must have the following structure:
 * <ul>
 * <li>opurl : string</li>
 * <li>nonce : string</li>
 * <li>date : datetime</li>
 * <li>primary key : opurl, nonce</li>
 * </ul>
 * </p>
 * 
 * @author Dmitry Mishin
 */

public class OpenidNonceVerifier implements NonceVerifier {

	private int maxAge;

	public OpenidNonceVerifier(int maxAge) {
		this.maxAge = maxAge;
	}

	@Override
	public int getMaxAge() {
		return maxAge;
	}

	@Override
	public int seen(final String opUrl, final String nonce) {
		cleanupAged();

    	return DbPoolServlet.goSql("Save openid nonce",
			"INSERT INTO openid_nonces VALUES (?,?, NOW())",
            new SqlWorker<Integer>() {
                @Override
                public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
                	try {
	                	stmt.setString(1, opUrl);
	                	stmt.setString(2, nonce);
	                	stmt.executeUpdate();
	            		return OK;
                	} catch(SQLException ex) {
                		if(isConstraintViolation(ex))
                			return SEEN;
                		return INVALID_TIMESTAMP;
            		}

                }
    		}
		);
	}

	public static boolean isConstraintViolation(SQLException e) {
	    return e.getSQLState().startsWith("23");
	}
	
	@Override
	public void setMaxAge(int ageSeconds) {
		this.maxAge = ageSeconds;
	}

	private void cleanupAged() {
		final Date boundary = new Date(System.currentTimeMillis() - 1000L * maxAge);
		DbPoolServlet.goSql("Cleanup openid nonces",
			"DELETE FROM openid_nonces WHERE date < ?",
	        new SqlWorker<Boolean>() {
	            @Override
	            public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
	            	stmt.setTimestamp(1, new Timestamp(boundary.getTime()));
	            	return stmt.execute();
	            }
			}
		);
	}
}
