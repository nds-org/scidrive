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
package edu.jhu.pha.vospace.meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.oauth.SciDriveUser;

/**
 * This class represents a metadata store for VOSpace based on the MySQL
 * open source database
 */
public class MySQLMetaStoreKeystone extends MySQLMetaStore2 {
	
	private static final Logger logger = Logger.getLogger(MySQLMetaStoreKeystone.class);
	private SciDriveUser owner;

	public MySQLMetaStoreKeystone(SciDriveUser username) {
		super(username);
		this.owner = username;
	}

	@Override
	public String createShare(final VospaceId identifier, final String groupId, final boolean write_perm)  {
		final String shareKey = RandomStringUtils.randomAlphanumeric(15);
		
		if(groupId != null && !groupId.isEmpty()) {
			DbPoolServlet.goSql("Adding new share for container",
	        		"insert into container_shares (share_id, container_id, group_id, share_write_permission) select ?, container_id, ?, ? from containers JOIN user_identities ON containers.user_id = user_identities.user_id WHERE identity = ? AND container_name = ?",
	                new SqlWorker<Integer>() {
	                    @Override
	                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
	                        stmt.setString(1, shareKey);
	                        stmt.setString(2, groupId);
	                        stmt.setBoolean(3, write_perm);
	                        stmt.setString(4, owner.getName());
	                        stmt.setString(5, identifier.getNodePath().getContainerName());
	                        return stmt.executeUpdate();
	                    }
	                }
	        );
		} else {
			DbPoolServlet.goSql("Adding new share for container",
	        		"insert into container_shares (share_id, container_id, share_write_permission) select ?, container_id, ? from containers JOIN user_identities ON containers.user_id = user_identities.user_id WHERE identity = ? AND container_name = ?",
	                new SqlWorker<Integer>() {
	                    @Override
	                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
	                        stmt.setString(1, shareKey);
	                        stmt.setBoolean(2, write_perm);
	                        stmt.setString(3, owner.getName());
	                        stmt.setString(4, identifier.getNodePath().getContainerName());
	                        return stmt.executeUpdate();
	                    }
	                }
	        );
		}
		return shareKey;
	}
}
