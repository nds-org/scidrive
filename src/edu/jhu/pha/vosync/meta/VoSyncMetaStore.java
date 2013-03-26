package edu.jhu.pha.vosync.meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;

public class VoSyncMetaStore {
	
	private String owner;
	
	public VoSyncMetaStore(String owner) {
		this.owner = owner;
	}

	public Chunk getLastChunk(final String chunkedId) {
        return DbPoolServlet.goSql("Retrieving last chunk from DB",
                "select max(chunked_num), sum(size) from `chunked_uploads` "+
        		"JOIN `user_identities` ON chunked_uploads.user_id = user_identities.user_id "+
        		"WHERE identity = ? and chunked_name = ?",
                new SqlWorker<Chunk>() {
                    @Override
                    public Chunk go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, owner);
                        stmt.setString(2, chunkedId);
                        ResultSet resSet = stmt.executeQuery();
                        if(resSet.next()) {
                        	int chunkNum = resSet.getInt(1)+1;
                        	if(resSet.wasNull())
                        		chunkNum = 0;

                        	int chunkPos = resSet.getInt(2);
                        	if(resSet.wasNull())
                        		chunkPos = 0;
                        	
                        	return new Chunk(chunkedId, chunkPos, chunkNum);
                        }
                        return null; // can't happen
                    }
                }
        );
	}
	
	public boolean putNewChunk(final Chunk chunk) {
        return DbPoolServlet.goSql("Adding new chunk to the database",
                "insert into chunked_uploads (chunked_name, chunked_num, user_id, size) select ?, ?, user_identities.user_id, ? from `user_identities` "+
        		"WHERE user_identities.identity = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, chunk.getChunkId());
                        stmt.setInt(2, chunk.getChunkNum());
                        stmt.setLong(3, chunk.getSize());
                        stmt.setString(4, owner);
                        return stmt.execute();
                    }
                }
        );
	}
	
}
