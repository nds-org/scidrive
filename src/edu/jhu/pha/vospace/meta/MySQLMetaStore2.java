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

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.Node.PropertyType;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodeInfo;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;

/**
 * This class represents a metadata store for VOSpace based on the MySQL
 * open source database
 */
public class MySQLMetaStore2 implements MetaStore{
	
	private static final Logger logger = Logger.getLogger(MySQLMetaStore2.class);
	private String owner;

	public MySQLMetaStore2(String username) {
		this.owner = username;
	}

	/*
	 * (non-Javadoc)
	 * @see edu.caltech.vao.vospace.meta.MetaStore#getNodeBytes(java.lang.String)
	 */
	@Override
	public byte[] getNodeBytes(final VospaceId identifier) {
        return DbPoolServlet.goSql("Retrieving node "+identifier+" for user "+owner+" from meta DB",
                "SELECT node FROM nodes JOIN containers ON nodes.container_id = containers.container_id JOIN user_identities ON containers.user_id = user_identities.user_id "+
                		"WHERE `current_rev` = 1 AND `container_name` = ? AND `path` = ? AND `identity` = ?",
                new SqlWorker<byte[]>() {
                    @Override
                    public byte[] go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, identifier.getNodePath().getContainerName());
                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(3, owner);
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next())
                            return rs.getBytes(1);
                        else {
                        	logger.error("Node "+identifier.toString()+" not found.");
                            throw new NotFoundException("NodeNotFound");
                        }
                    }
                }
        );
	}

	/*
	 * (non-Javadoc)
	 * @see edu.caltech.vao.vospace.meta.MetaStore#getNodeChildren(edu.jhu.pha.vospace.node.VospaceId, boolean)
	 */
	@Override
	public NodesList getNodeChildren(final VospaceId identifier, final boolean searchDeep /*always false*/, final boolean includeDeleted /*not used*/, final int start, final int count) {
        if(identifier.getNodePath().isRoot(false)) {
        	String deletedCondition = includeDeleted?"":"nodes.`deleted` = 0 AND ";
    		return DbPoolServlet.goSql("GetNodeChildren root request",
    				"SELECT SQL_CALC_FOUND_ROWS containers.container_name as container, nodes.rev, nodes.deleted, nodes.mtime, nodes.size, nodes.mimetype, nodes.type "+
            		"FROM nodes JOIN containers ON nodes.container_id = containers.container_id JOIN user_identities ON containers.user_id = user_identities.user_id "+
            		"WHERE "+deletedCondition+"`parent_node_id` is NULL AND `identity` = ? AND `container_name` <> '' order by container "+((count > 0)?" limit ?, ?":""),
                    new SqlWorker<NodesList>() {
                        @Override
                        public NodesList go(Connection conn, PreparedStatement stmt) throws SQLException {
                    		ArrayList<Node> result = new ArrayList<Node>();
                    		int countRows = 0;

                    		stmt.setString(1, owner);
                            
                    		if(count > 0) {
                            	stmt.setInt(2, start);
                            	stmt.setInt(3, count);
                            }
                    		
                            ResultSet rs = stmt.executeQuery();
                			while (rs.next()) {
        			    		try {
        			    			VospaceId id = new VospaceId(new NodePath(rs.getString("container")));
        			    			id.getNodePath().setEnableAppContainer(identifier.getNodePath().isEnableAppContainer());
        			    			
        			    			NodeInfo info = new NodeInfo();
                                	info.setRevision(rs.getInt("rev"));
                                	info.setDeleted(rs.getBoolean("deleted"));
                                	info.setMtime(new Date(rs.getTimestamp("mtime").getTime()));
                                	info.setSize(rs.getLong("size"));
                                	info.setContentType(rs.getString("mimetype"));

        			    			Node newNode = NodeFactory.createNode(id, owner, NodeType.valueOf(rs.getString("type")));
        			    			newNode.setNodeInfo(info);
                                	
    								result.add(newNode);
    							} catch (URISyntaxException e) {
    								logger.error("Error in child URI: "+e.getMessage());
    							}
                			}
                			
                			ResultSet resSet = conn.createStatement().executeQuery("SELECT FOUND_ROWS();");
                			resSet.next();
                			countRows = resSet.getInt(1);
                			
                			return new NodesList(result, countRows);
                        }
                    }
            );
        } else {
        	String deletedCondition = includeDeleted?"":"nodes.`deleted` = 0 AND ";
			String request = "SELECT SQL_CALC_FOUND_ROWS containers.container_name as container, nodes.path, nodes.rev, nodes.deleted, nodes.mtime, nodes.size, nodes.mimetype, nodes.type "+
	        		"FROM nodes JOIN containers ON nodes.container_id = containers.container_id "+
					"JOIN user_identities ON containers.user_id = user_identities.user_id "+
	        		"JOIN nodes b ON nodes.parent_node_id = b.node_id "+
	        		"WHERE "+deletedCondition+"containers.container_name = ? AND b.`path` = ? AND `identity` = ? order by path "+((count > 0)?" limit ?, ?":"");
	        
			return DbPoolServlet.goSql("GetNodeChildren request",
	        		request,
	                new SqlWorker<NodesList>() {
	                    @Override
	                    public NodesList go(Connection conn, PreparedStatement stmt) throws SQLException {
	                		ArrayList<Node> result = new ArrayList<Node>();
                    		int countRows = 0;
                    		
	                		stmt.setString(1, identifier.getNodePath().getContainerName());
	                		stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
	                		stmt.setString(3, owner);

                    		if(count > 0) {
                            	stmt.setInt(4, start);
                            	stmt.setInt(5, count);
                            }

	                        ResultSet rs = stmt.executeQuery();
	            			while (rs.next()) {
	    			    		try {
	    			    			VospaceId id = new VospaceId(new NodePath(rs.getString("container")+"/"+rs.getString("path")));
	    			    			id.getNodePath().setEnableAppContainer(identifier.getNodePath().isEnableAppContainer());
	    			    			
	    			    			NodeInfo info = new NodeInfo();
	                            	info.setRevision(rs.getInt("rev"));
	                            	info.setDeleted(rs.getBoolean("deleted"));
	                            	info.setMtime(new Date(rs.getTimestamp("mtime").getTime()));
	                            	info.setSize(rs.getLong("size"));
	                            	info.setContentType(rs.getString("mimetype"));
	
	    			    			Node newNode = NodeFactory.createNode(id, owner, NodeType.valueOf(rs.getString("type")));
	    			    			newNode.setNodeInfo(info);
	                            	
									result.add(newNode);
								} catch (URISyntaxException e) {
									logger.error("Error in child URI: "+e.getMessage());
								}
	            			}

                			ResultSet resSet = conn.createStatement().executeQuery("SELECT FOUND_ROWS();");
                			resSet.next();
                			countRows = resSet.getInt(1);
                			
                			return new NodesList(result, countRows);
	                    }
	                }
	        );
        }
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.MetaStore#getNodeInfo(edu.jhu.pha.vospace.node.VospaceId)
	 */
	@Override
	public NodeInfo getNodeInfo(final VospaceId identifier) {
        return DbPoolServlet.goSql("Retrieving node info",
        		"select rev, deleted, nodes.mtime, nodes.size, mimetype, chunked_name from nodes " +
        		"JOIN containers ON nodes.container_id = containers.container_id " +
        		"JOIN user_identities ON containers.user_id = user_identities.user_id "+
        		"LEFT JOIN chunked_uploads ON nodes.node_id = chunked_uploads.node_id "+
                "WHERE current_rev = 1 and container_name = ? and path = ? and identity = ?",
                new SqlWorker<NodeInfo>() {
                    @Override
                    public NodeInfo go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	NodeInfo info = new NodeInfo();

                        stmt.setString(1, identifier.getNodePath().getContainerName());
                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(3, owner);

                        ResultSet resSet = stmt.executeQuery();
                        
                        if(resSet.next()) {
                        	info.setRevision(resSet.getInt("rev"));
                        	info.setDeleted(resSet.getBoolean("deleted"));
                        	info.setMtime(new Date(resSet.getTimestamp("mtime").getTime()));
                        	info.setSize(resSet.getLong("size"));
                        	info.setContentType(resSet.getString("mimetype"));
                        	info.setChunkedName(resSet.getString("chunked_name"));
                        } else {
                        	throw new NotFoundException("NodeNotFound");
                        }
                    	
                    	return info;
                    }
                }
        );	
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.MetaStore#getType(edu.jhu.pha.vospace.node.VospaceId)
	 */
	@Override
	public NodeType getType(final VospaceId identifier)  {
        return DbPoolServlet.goSql("Get node type",
        		"select nodes.type from nodes JOIN containers ON nodes.container_id = containers.container_id JOIN user_identities ON containers.user_id = user_identities.user_id "+
        				"WHERE `current_rev` = 1 and `container_name` = ? and `path` = ? and `identity` = ?",
                new SqlWorker<NodeType>() {
                    @Override
                    public NodeType go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, identifier.getNodePath().getContainerName());
                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(3, owner);
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()) {
                            return NodeType.valueOf(NodeType.class, rs.getString(1));
                        } else {
                            throw new NotFoundException("NodeNotFound");
                        }
                    }
                }
        );
	}
	
	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.MetaStore#incrementRevision(edu.jhu.pha.vospace.node.VospaceId)
	 */
	/*@Override
	public void incrementRevision(final VospaceId identifier) {
		
        DbPoolServlet.goSql("Updating node "+identifier+" current revision",
                "UPDATE nodes SET rev = rev+1 "+
                		"WHERE current_rev = 1 AND node_id = "+
                		"(SELECT * FROM (SELECT nodes.node_id FROM nodes JOIN containers "+
                		"ON nodes.container_id = containers.container_id JOIN user_identities "+
                		"ON containers.user_id = user_identities.user_id WHERE `container_name` = ? AND `path` = ? AND `identity` = ?) a)",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, identifier.getNodePath().getContainerName());
                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(3, owner);
                        int upd = stmt.executeUpdate();
                        if (upd > 0)
                            return true;
                        else {
                        	logger.error("Node "+identifier.toString()+" not found in incrementRevision.");
                            throw new NotFoundException("NodeNotFound");
                        }
                    }
                }
        );
	}*/


	/*
	 * (non-Javadoc)
	 * @see edu.caltech.vao.vospace.meta.MetaStore#isStored(java.lang.String)
	 */
	@Override
	public boolean isStored(final VospaceId identifier) {
        return DbPoolServlet.goSql("Is node stored?",
        		"select count(*) from nodes "+
        				"JOIN containers ON nodes.container_id = containers.container_id JOIN user_identities ON containers.user_id = user_identities.user_id "+
                		"WHERE `current_rev` = 1 AND `container_name` = ? AND `path` = ? AND `identity` = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, identifier.getNodePath().getContainerName());
                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(3, owner);
                        ResultSet rs = stmt.executeQuery();
                        rs.next();
                        return rs.getInt(1)>0;
                    }
                }
        );
	}


	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.MetaStore#markRemoved(edu.jhu.pha.vospace.node.VospaceId, boolean)
	 */
	@Override
	public void markRemoved(final VospaceId identifier, final boolean isRemoved) {
		if(identifier.getNodePath().isRoot(false))
			return;
        DbPoolServlet.goSql("Marking node as removed",
        		"update nodes set deleted = ? "+
        		"WHERE node_id = "+
        		"(SELECT * FROM (SELECT nodes.node_id FROM nodes JOIN containers "+
        		"ON nodes.container_id = containers.container_id JOIN user_identities "+
        		"ON containers.user_id = user_identities.user_id WHERE `container_name` = ? AND `path` = ? AND `identity` = ?) a)",
                new SqlWorker<Integer>() {
                    @Override
                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setBoolean(1, isRemoved);
                        stmt.setString(2, identifier.getNodePath().getContainerName());
                        stmt.setString(3, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(4, owner);
                        return stmt.executeUpdate();
                    }
                }
        );
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.MetaStore#remove(edu.jhu.pha.vospace.node.VospaceId)
	 */
	@Override
	public void remove(final VospaceId identifier) {
		if (identifier.getNodePath().getNodeRelativeStoragePath().isEmpty()) {
	        DbPoolServlet.goSql("Removing root"+identifier,
	        		"delete from containers "+
	        		"WHERE container_id in "+
	        		"(SELECT * FROM (SELECT containers.container_id FROM containers JOIN user_identities "+
	        		"ON containers.user_id = user_identities.user_id WHERE `container_name` = ? AND `identity` = ?) a)",
	        
	                new SqlWorker<Integer>() {
	                    @Override
	                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
	                        stmt.setString(1, identifier.getNodePath().getContainerName());
	                        stmt.setString(2, owner);
	                        return stmt.executeUpdate();
	                    }
	                }
	        );
		} else {
	        DbPoolServlet.goSql("Removing non-root"+identifier,
	        		"delete from nodes "+
	        		"WHERE node_id in "+
	        		"(SELECT * FROM (SELECT nodes.node_id FROM nodes "+
	        		"JOIN containers ON nodes.container_id = containers.container_id "+
	        		"JOIN user_identities ON containers.user_id = user_identities.user_id "+
	        		"WHERE `container_name` = ? AND `path` = ? AND `identity` = ?) a)",
	        
	                new SqlWorker<Integer>() {
	                    @Override
	                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
	                        stmt.setString(1, identifier.getNodePath().getContainerName());
	                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
	                        stmt.setString(3, owner);
	                        return stmt.executeUpdate();
	                    }
	                }
	        );
		}
	}
	

	

	//TODO edit method for new db (cont+path)
	/*
	 * (non-Javadoc)
	 * @see edu.caltech.vao.vospace.meta.MetaStore#search(edu.jhu.pha.vospace.node.VospaceId, java.lang.String, int, boolean)
	 */
	@Override
	public List<VospaceId> search(final VospaceId identifier, final String searchPattern, final int fileLimit, final boolean includeDeleted) {
        String request = "select container, path from nodes where "+(includeDeleted?"":"deleted = 0 and ") + " current_rev = 1 and owner = ? and container = ? and path like ? and path regexp ? order by path limit ?";
        
		return DbPoolServlet.goSql("search request",
        		request,
                new SqlWorker<List<VospaceId>>() {
                    @Override
                    public List<VospaceId> go(Connection conn, PreparedStatement stmt) throws SQLException {
                		ArrayList<VospaceId> result = new ArrayList<VospaceId>();

                		stmt.setString(1, owner);
                        stmt.setString(2, identifier.getNodePath().getContainerName());
                        stmt.setString(3, identifier.getNodePath().getNodeRelativeStoragePath()+"%");
                        stmt.setString(4, "^"+identifier.getNodePath().getNodeRelativeStoragePath()+".*"+searchPattern+".*");
                        stmt.setInt(5, fileLimit);
                        ResultSet rs = stmt.executeQuery();
            			while (rs.next()) {
    			    		try {
    			    			NodePath npath = new NodePath("/"+rs.getString(1)+"/"+rs.getString(2));
								result.add(new VospaceId(npath));
							} catch (URISyntaxException e) {
								logger.error("Error in child URI: "+e.getMessage());
							}
            			}
            			return result;
                    }
                }
        );
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.MetaStore#storeData(edu.jhu.pha.vospace.node.VospaceId, edu.jhu.pha.vospace.node.NodeType, java.lang.Object)
	 */
	@Override
	public void storeData(final VospaceId identifier, final NodeType type)  {
		DbPoolServlet.goSql("Adding container",
        		"insert ignore into containers (container_name, user_id) select ?, user_id from user_identities where identity = ?",
                new SqlWorker<Integer>() {
                    @Override
                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, identifier.getNodePath().getContainerName());
                        stmt.setString(2, owner);
                        return stmt.executeUpdate();
                    }
                }
        );
		
		DbPoolServlet.goSql("Adding metadata",
        		"insert into nodes (container_id, path, parent_node_id, type, mimetype) "+
        				"SELECT containers.`container_id`, ?, nodes.node_id, ?, ? FROM containers "+
        				"JOIN user_identities ON containers.`user_id` = user_identities.user_id "+
        				"LEFT JOIN nodes ON containers.`container_id` = nodes.`container_id` and nodes.path = ? "+
        				"WHERE `identity` = ? and `container_name` = ?",
        				new SqlWorker<Integer>() {
                    @Override
                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	
                    	String mimeType = "";
                    	if(type == NodeType.DATA_NODE || type == NodeType.STRUCTURED_DATA_NODE || type == NodeType.UNSTRUCTURED_DATA_NODE)
                    		mimeType = "application/file";
                    	
                        stmt.setString(1, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(2, type.name());
                        stmt.setString(3, mimeType);
                        stmt.setString(4, identifier.getNodePath().getParentPath().getNodeRelativeStoragePath());
                        stmt.setString(5, owner);
                        stmt.setString(6, identifier.getNodePath().getContainerName());
                        return stmt.executeUpdate();
                    }
                }
        );
	}

	@Override
	public void storeInfo(final VospaceId identifier, final NodeInfo info) {
        DbPoolServlet.goSql("Adding nodeinfo",
        		"update nodes set size = ?, mimetype = ?, rev = ? where current_rev = 1 and node_id = "+
                		"(SELECT * FROM (SELECT nodes.node_id FROM nodes JOIN containers "+
                		"ON nodes.container_id = containers.container_id JOIN user_identities "+
                		"ON containers.user_id = user_identities.user_id WHERE `container_name` = ? AND `path` = ? AND `identity` = ?) a)",
                new SqlWorker<Integer>() {
                    @Override
                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setLong(1, info.getSize());
                        stmt.setString(2, info.getContentType());
                        stmt.setInt(3, info.getRevision());
                        stmt.setString(4, identifier.getNodePath().getContainerName());
                        stmt.setString(5, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(6, owner);
                        return stmt.executeUpdate();
                    }
                }
        );
      
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.MetaStore#makeStructured(edu.jhu.pha.vospace.node.VospaceId, boolean)
	 */
	@Override
	public void makeStructured(final VospaceId identifier, final boolean isStructured) {
        DbPoolServlet.goSql("Adding nodeinfo",
        		"update nodes set type = ? where current_rev = 1 and node_id = "+
                		"(SELECT * FROM (SELECT nodes.node_id FROM nodes JOIN containers "+
                		"ON nodes.container_id = containers.container_id JOIN user_identities "+
                		"ON containers.user_id = user_identities.user_id WHERE `container_name` = ? AND `path` = ? AND `identity` = ?) a)",
                new SqlWorker<Integer>() {
                    @Override
                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, (isStructured)?NodeType.STRUCTURED_DATA_NODE.toString():NodeType.UNSTRUCTURED_DATA_NODE.toString());
                        stmt.setString(2, identifier.getNodePath().getContainerName());
                        stmt.setString(3, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(4, owner);
                        return stmt.executeUpdate();
                    }
                }
        );
      
	}

	/*
	 * (non-Javadoc)
	 * @see edu.jhu.pha.vospace.meta.MetaStore#updateData(edu.jhu.pha.vospace.node.VospaceId, edu.jhu.pha.vospace.node.VospaceId)
	 */
	@Override
	public void updateData(final VospaceId identifier, final VospaceId newIdentifier)  {
        DbPoolServlet.goSql("Updating metadata",
        		"update nodes set container_id = SELECT container_id from containers JOIN user_identities ON containers.user_id = user_identities.user_id WHERE identity = ? AND container_name = ?, "+
        				"path = ? where current_rev = 1 and node_id = "+
                		"(SELECT * FROM (SELECT nodes.node_id FROM nodes JOIN containers "+
                		"ON nodes.container_id = containers.container_id JOIN user_identities "+
                		"ON containers.user_id = user_identities.user_id WHERE `container_name` = ? AND `path` = ? AND `identity` = ?) a)",
                new SqlWorker<Integer>() {
                    @Override
                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.setString(1, owner);
                        stmt.setString(2, newIdentifier.getNodePath().getContainerName());
                        stmt.setString(3, newIdentifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(4, identifier.getNodePath().getContainerName());
                        stmt.setString(5, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(6, owner);
                        return stmt.executeUpdate();
                    }
                }
        );
	}

	@Override
	public void updateUserProperties(final VospaceId identifier, final Map<String, String> properties)  {
        DbPoolServlet.goSql("Updating properties",
        		"INSERT IGNORE INTO properties (property_uri) VALUES (?)",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	for(String uri: properties.keySet()) {
                    		stmt.setString(1, uri);
                    		stmt.executeUpdate();
                    	}
                        return true;
                    }
                }
        );
        DbPoolServlet.goSql("Updating properties",
        		"INSERT INTO node_properties (node_id, property_id, property_value) SELECT `node_id`, `property_id`, ? FROM nodes "+
        					"JOIN containers ON nodes.container_id = containers.container_id "+
        					"JOIN user_identities ON containers.user_id = user_identities.user_id "+
        					"JOIN properties " +
	                		"WHERE `container_name` = ? AND `path` = ? AND `identity` = ? AND `property_uri` = ? AND `property_readonly` = 0 "+
        					"ON DUPLICATE KEY UPDATE property_value = ?",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	for(String uri: properties.keySet()) {
                    		if(null != properties.get(uri)) {
	                    		stmt.setString(1, properties.get(uri));
		                        stmt.setString(2, identifier.getNodePath().getContainerName());
		                        stmt.setString(3, identifier.getNodePath().getNodeRelativeStoragePath());
		                        stmt.setString(4, owner);
	                    		stmt.setString(5, uri);
	                    		stmt.setString(6, properties.get(uri));
	                    		stmt.executeUpdate();
                    		}
                    	}
                        return true;
                    }
                }
        );
        DbPoolServlet.goSql("Deleting properties",
        		"DELETE from node_properties WHERE node_id = (SELECT nodes.`node_id` FROM nodes "+
        		"JOIN containers ON nodes.container_id = containers.container_id "+ 
        		"JOIN user_identities ON containers.user_id = user_identities.user_id "+ 
        		"WHERE `container_name` = ? AND `path` = ? AND `identity` = ?) "+
        		"AND `property_id` = (SELECT property_id FROM properties WHERE property_uri = ? and `property_readonly` = 0)",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	for(String uri: properties.keySet()) {
                    		if(null == properties.get(uri)) {
		                        stmt.setString(1, identifier.getNodePath().getContainerName());
		                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
		                        stmt.setString(3, owner);
	                    		stmt.setString(4, uri);
	                    		stmt.executeUpdate();
                    		}
                    	}
                        return true;
                    }
                }
        );
	}
	
	@Override
	public String createShare(final VospaceId identifier, final String groupId, final boolean write_perm)  {
		final String shareKey = RandomStringUtils.randomAlphanumeric(15);
		
		if(groupId != null && !groupId.isEmpty()) {
			DbPoolServlet.goSql("Adding new share for container",
	        		"insert into container_shares (share_id, container_id, group_id, share_write_permission) select ?, container_id, group_id, ? from containers JOIN user_identities ON containers.user_id = user_identities.user_id JOIN groups WHERE identity = ? AND container_name = ? and group_id = ?",
	                new SqlWorker<Integer>() {
	                    @Override
	                    public Integer go(Connection conn, PreparedStatement stmt) throws SQLException {
	                        stmt.setString(1, shareKey);
	                        stmt.setBoolean(2, write_perm);
	                        stmt.setString(3, owner);
	                        stmt.setString(4, identifier.getNodePath().getContainerName());
	                        stmt.setString(5, groupId);
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
	                        stmt.setString(3, owner);
	                        stmt.setString(4, identifier.getNodePath().getContainerName());
	                        return stmt.executeUpdate();
	                    }
	                }
	        );
		}
		return shareKey;
	}

	@Override
	public Map<String, String> getProperties(final VospaceId identifier) {
		return DbPoolServlet.goSql("Get node properties",
        		"select property_uri, property_value from node_properties "+
        		"JOIN properties ON node_properties.property_id = properties.property_id "+
        		"JOIN nodes ON node_properties.node_id = nodes.node_id " +
				"JOIN containers ON nodes.container_id = containers.container_id "+
        		"JOIN user_identities ON containers.user_id = user_identities.user_id "+
        		"WHERE `current_rev` = 1 AND `container_name` = ? AND `path` = ? AND `identity` = ?",
                new SqlWorker<Map<String, String>>() {
                    @Override
                    public Map<String, String> go(Connection conn, PreparedStatement stmt) throws SQLException {
                    	
                    	Map<String, String> map = new HashMap<String, String>();
                        stmt.setString(1, identifier.getNodePath().getContainerName());
                        stmt.setString(2, identifier.getNodePath().getNodeRelativeStoragePath());
                        stmt.setString(3, owner);
                        ResultSet resSet = stmt.executeQuery();
                        while(resSet.next()) {
                        	map.put(resSet.getString("property_uri"), resSet.getString("property_value"));
                        }
                        return map;
                    }
                }
        );
	}
}
