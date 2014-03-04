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
package edu.jhu.pha.vospace;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.HOURS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import javax.servlet.http.HttpServlet;

import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.node.ContainerNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.oauth.SciDriveUser;
import edu.jhu.pha.vospace.storage.StorageManager;
import edu.jhu.pha.vospace.storage.StorageManagerFactory;

public class DbCleanerServlet extends HttpServlet {
	
	private static final long serialVersionUID = -6837095401346471188L;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

    private ScheduledFuture<?> nodesCleanerHandle, dbCleanerHandle, chunksCleanerHandle;
    
    private final int NODE_EXPIRY_INTERVAL = SettingsServlet.getConfig().getInt("node_expiry", 10);
    private final int NODES_RUN_PERIOD = SettingsServlet.getConfig().getInt("cleaner_period", 1);
    private final int DB_RUN_PERIOD = SettingsServlet.getConfig().getInt("db_cleaner_period", 12);
    private final int CHUNK_EXPIRY_INTERVAL = SettingsServlet.getConfig().getInt("chunk_expiry", 1440);
    private final int CHUNK_RUN_PERIOD = SettingsServlet.getConfig().getInt("chunk_period", 1);

    private static final Logger logger = Logger.getLogger(DbCleanerServlet.class);
    
    @Override
	public void init() {
        final Runnable nodescleaner = new NodesRemover();
        nodesCleanerHandle = scheduler.scheduleAtFixedRate(nodescleaner, (long)(Math.random()*NODES_RUN_PERIOD), NODES_RUN_PERIOD, MINUTES);
        final Runnable dbcleaner = new DBCleaner();
        dbCleanerHandle = scheduler.scheduleAtFixedRate(dbcleaner, (long)(Math.random()*DB_RUN_PERIOD), DB_RUN_PERIOD, HOURS);
        final Runnable chunkscleaner = new ChunksRemover();
        chunksCleanerHandle = scheduler.scheduleAtFixedRate(chunkscleaner, (long)(Math.random()*CHUNK_RUN_PERIOD), CHUNK_RUN_PERIOD, HOURS);
    }

    @Override
    public void destroy() {
    	nodesCleanerHandle.cancel(true);
    	dbCleanerHandle.cancel(true);
    	chunksCleanerHandle.cancel(true);
    	scheduler.shutdownNow();
    	logger.info("Cleaner is terminating");
    }
        
    private class NodesRemover implements Runnable {
        @Override
		public void run() {
        	boolean res = true;
        	while(res) {
            	res = DbPoolServlet.goSql("Cleaning DB nodes",
                		"select container_name, path, identity from nodes " +
                		"JOIN containers ON nodes.container_id = containers.container_id " +
                		"JOIN user_identities ON containers.user_id = user_identities.user_id " +
                		"where deleted = 1 and mtime < (NOW() - INTERVAL "+NODE_EXPIRY_INTERVAL+" MINUTE) order by path limit 1",
                        new SqlWorker<Boolean>() {
                            @Override
                            public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                                ResultSet resSet = stmt.executeQuery();
                                if(resSet.next()) {
                                	SciDriveUser username = SciDriveUser.fromName(resSet.getString("identity"));
                                	
                                	Node newNode = null;
                                	try {
	                                	VospaceId uri = new VospaceId(new NodePath(resSet.getString("container_name")+"/"+resSet.getString("path")));
	                                	
	                                	logger.debug("Removing "+uri.toString()+"of user "+username);
	                                	
	                                	newNode = NodeFactory.getNode(uri, username);

	                    				newNode.getStorage().remove(newNode.getUri().getNodePath(), true);
                    					newNode.getMetastore().remove(newNode.getUri());

                    					if(!newNode.getUri().getNodePath().isRoot(false)) {
		                    				// Update root container size
	                    					ContainerNode contNode = (ContainerNode)NodeFactory.getNode(
	                    							new VospaceId(new NodePath(newNode.getUri().getNodePath().getContainerName())), 
	                    							newNode.getOwner());
	                    					newNode.getStorage().updateNodeInfo(contNode.getUri().getNodePath(), contNode.getNodeInfo());
	                    					newNode.getMetastore().storeInfo(contNode.getUri(), contNode.getNodeInfo());
                    					}
                    				} catch(Exception ex) {
                                		ex.printStackTrace();
                                		if(null != newNode) {
                                			newNode.markRemoved(false);
                                			logger.error("Error removing the node "+newNode.getUri()+" : "+ex.getMessage());
                                		}
                                	}
	                            	return true;
                                }
                            	return false;
                            }
                        }
                );
        	}
        }
    }

    private class ChunksRemover implements Runnable {
        @Override
		public void run() {
        	boolean res = true;
        	while(res) {
            	res = DbPoolServlet.goSql("Cleaning chunks",
        			"select `identity`, `chunked_name` from (select `chunked_name`, max(`mtime`) `maxtime`, `identity` from chunked_uploads "+ 
        			"JOIN user_identities ON chunked_uploads.`user_id` = user_identities.`user_id` "+
        			"where `node_id` is NULL group by `chunked_name`) a WHERE `maxtime` < (NOW() - INTERVAL "+CHUNK_EXPIRY_INTERVAL+" MINUTE) limit 1", 
                    new SqlWorker<Boolean>() {
                        @Override
                        public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                            ResultSet resSet = stmt.executeQuery();
                            if(resSet.next()) {
                            	SciDriveUser username = SciDriveUser.fromName(resSet.getString("identity"));
                            	final String chunkedName = resSet.getString("chunked_name");
                            	logger.debug("Removing "+chunkedName+" chunks of user "+username);
                            	
                            	StorageManager storage = StorageManagerFactory.getStorageManager(username);
                            	storage.removeObjectSegment(resSet.getString("chunked_name"));

                            	DbPoolServlet.goSql("Deleting unused chunked upload",
                            		"delete from chunked_uploads where chunked_name = ?", 
                                    new SqlWorker<Boolean>() {
                                        @Override
                                        public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                                        	stmt.setString(1, chunkedName);
                                        	stmt.executeUpdate();
                                        	return true;
                                        }
                                    }
                    			);
                            	return true;
                            }
                        	return false;
                        }
                    }
                );
        	}
        }
    }

    private class DBCleaner implements Runnable {
        @Override
		public void run() {
            DbPoolServlet.goSql("Cleaning DB oauth nonces",
        		"delete from oauth_nonces where timestamp < (UNIX_TIMESTAMP(NOW())-86400)*1000",
                new SqlWorker<Boolean>() {
                    @Override
                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                        stmt.executeUpdate();
                    	return true;
                    }
            	}
            );
        }
    }

}


