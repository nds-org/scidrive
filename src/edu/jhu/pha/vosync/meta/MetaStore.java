package edu.jhu.pha.vosync.meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;

public class MetaStore {
	
	private String owner;
	
	public MetaStore(String owner) {
		this.owner = owner;
	}
	
	public boolean putNewChunk(final String chunkId) {
        return DbPoolServlet.goSql("Adding new chunk to the database",
                "insert into chunked_uploads select NULL, ?, max(chunked_num)+1, user_identities.user_id, NULL, NULL from `chunked_uploads` "+
        		"JOIN `user_identities` ON chunked_uploads.user_id = user_identities.user_id "+
        		"WHERE identity = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, chunkId);
                        stmt.setString(2, owner);
                        return stmt.executeUpdate() > 0;
                    }
                }
        );
	}

}
