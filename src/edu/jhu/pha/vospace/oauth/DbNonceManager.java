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
package edu.jhu.pha.vospace.oauth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;

public final class DbNonceManager {
    private final long maxAge;

    public DbNonceManager(long maxAge) {
        this.maxAge = maxAge;
    }

    public synchronized boolean verify(final String timestamp, final String nonce) {
        long now = System.currentTimeMillis();

        final long stamp = Long.valueOf(timestamp) * 1000;

        // invalid timestamp supplied; automatically invalid
        if (stamp + maxAge < now) {
            return false;
        }

    	boolean result = DbPoolServlet.goSql("Get oauth nonce",
        		"insert ignore into oauth_nonces (timestamp, nonce) values (?, ?)",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	stmt.setLong(1, stamp);
                    	stmt.setString(2, nonce);
                    	return stmt.executeUpdate() > 0;
                    }
                }
        );

        return result;
    }
}

