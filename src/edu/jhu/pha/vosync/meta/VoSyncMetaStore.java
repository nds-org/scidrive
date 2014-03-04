package edu.jhu.pha.vosync.meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.oauth.SciDriveUser;

public class VoSyncMetaStore {
	
	private SciDriveUser owner;
	
	public VoSyncMetaStore(SciDriveUser owner) {
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
                        stmt.setString(1, owner.getName());
                        stmt.setString(2, chunkedId);
                        ResultSet resSet = stmt.executeQuery();
                        if(resSet.next()) {
                        	int chunkNum = resSet.getInt(1)+1;
                        	if(resSet.wasNull())
                        		chunkNum = 0;

                        	long chunkPos = resSet.getLong(2);
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
                        stmt.setString(4, owner.getName());
                        return stmt.execute();
                    }
                }
        );
	}

	public boolean chunkedExists(final String uploadId) {
        return DbPoolServlet.goSql("Checking chunked upload to exist in DB",
                "select count(chunked_name) from `chunked_uploads` "+
           		"JOIN `user_identities` ON chunked_uploads.user_id = user_identities.user_id "+
        		"WHERE identity = ? and chunked_name = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, owner.getName());
                        stmt.setString(2, uploadId);
                        ResultSet resSet = stmt.executeQuery();
                        if(resSet.next()) {
                        	return resSet.getInt(1) > 0;
                        } else { // can't happen
                        	return false;
                        }
                    }
                }
        );
	}
	
	public boolean mapChunkedToNode(final VospaceId identifier, final String chunkedId) {
        return DbPoolServlet.goSql("Mapping chunked to node",
        		"update chunked_uploads set node_id = "+
        		"(SELECT nodes.node_id FROM nodes "+
        		"JOIN containers ON nodes.container_id = containers.container_id "+
        		"JOIN user_identities ON containers.user_id = user_identities.user_id "+
        		"WHERE `container_name` = ? AND `path` = ? AND `identity` = ?) "+
        		"WHERE chunked_name = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, identifier.getNodePath().getContainerName());
                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(3, owner.getName());
                        stmt.setString(4, chunkedId);
                        return stmt.execute();
                    }
                }
        );
	}
	
	public boolean deleteNodeChunks(final VospaceId identifier) {
        return DbPoolServlet.goSql("Deleting node chunks",
        		"delete from chunked_uploads where node_id = "+
        		"(SELECT nodes.node_id FROM nodes "+
        		"JOIN containers ON nodes.container_id = containers.container_id "+
        		"JOIN user_identities ON containers.user_id = user_identities.user_id "+
        		"WHERE `container_name` = ? AND `path` = ? AND `identity` = ?) ",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, identifier.getNodePath().getContainerName());
                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(3, owner.getName());
                        return stmt.execute();
                    }
                }
        );
	}
	
}
