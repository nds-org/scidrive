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

public class DbCleanerServlet extends HttpServlet {
	
	private static final long serialVersionUID = -6837095401346471188L;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private ScheduledFuture<?> cleanerHandle;
    
    private final int NODE_EXPIRY_INTERVAL = SettingsServlet.getConfig().getInt("node_expiry", 10);
    private final int RUN_PERIOD = SettingsServlet.getConfig().getInt("cleaner_period", 1);

    private static final Logger logger = Logger.getLogger(DbCleanerServlet.class);
    
    @Override
	public void init() {
        final Runnable cleaner = new Runnable() {
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
	                                	String username = resSet.getString("identity");
	                                	
	                                	try {
		                                	VospaceId uri = new VospaceId(new NodePath(resSet.getString("container_name")+"/"+resSet.getString("path")));
		                                	
		                                	logger.debug("Removing "+uri.toString()+"of user "+username);
		                                	
		                                	Node newNode = NodeFactory.getNode(uri, username);
	
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
	                                	}
		                            	return true;
	                                }
	                            	return false;
	                            }
	                        }
	                );
            	}
            }
        };

        cleanerHandle =
            scheduler.scheduleAtFixedRate(cleaner, 1, RUN_PERIOD, MINUTES);
    }

    @Override
    public void destroy() {
    	cleanerHandle.cancel(true);
    	scheduler.shutdownNow();
    	logger.info("Cleaner is terminating");
    }
        

}


