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

public class SynchronizationServlet extends HttpServlet {
	
	private static final long serialVersionUID = -6837095401346471188L;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    private ScheduledFuture<?> cleanerHandle;
    
	private static final Logger logger = Logger.getLogger(SynchronizationServlet.class);
    @Override
	public void init() {
        final Runnable cleaner = new Runnable() {
            public void run() {
                final String region = SettingsServlet.getConfig().getString("region"); 
                
                /* Add SWIFT storage URL to records where syncregion is local for the cluster */
                DbPoolServlet.goSql("Check missing sync urls",
                		"select container, owner, region from cont_loc where syncregion = ? and syncurl is NULL",
                        new SqlWorker<Boolean>() {
                            @Override
                            public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                            	stmt.setString(1, region);
                                ResultSet resSet = stmt.executeQuery();

                            	PreparedStatement pstmt = conn.prepareStatement("update cont_loc set syncurl = ? where container = ? and owner = ? and region = ?");

                                while(resSet.next()) {
                                	String containerName = resSet.getString("container");
                                	String username = resSet.getString("owner");
                                	String region = resSet.getString("region");
                                	
                                	try {
                                		NodePath path = new NodePath(containerName);
	                                	VospaceId uri = new VospaceId(path);
	                                	Node newNode = NodeFactory.getNode(uri, username);
	                                	
	                                	pstmt.setString(1, newNode.getStorage().getStorageUrl()+"/"+containerName);
	                                	pstmt.setString(2, containerName);
	                                	pstmt.setString(3, username);
	                                	pstmt.setString(4, region);
	                                	pstmt.execute();
                                	} catch(Exception ex) {
                                		ex.printStackTrace();
                                	}
                                }
                            	pstmt.close();
                            	return true;
                            }
                        }
                );

                /* Check where storage URL s different from the one in SWIFT and modify if necessary */
                DbPoolServlet.goSql("Check nodes sync",
                		"select container, owner, syncregion, syncurl, synckey from cont_loc where region = ?",
                        new SqlWorker<Boolean>() {
                            @Override
                            public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                            	stmt.setString(1, region);
                                ResultSet resSet = stmt.executeQuery();
                                while(resSet.next()) {
                                	String containerName = resSet.getString("container");
                                	String username = resSet.getString("owner");
                                	String syncRegion = resSet.getString("syncregion");
                                	String syncUrl = resSet.getString("syncurl");
                                	String syncKey = resSet.getString("synckey");
                                	
                                	try {
                                		NodePath path = new NodePath(containerName);
	                                	VospaceId uri = new VospaceId(path);
	                                	Node newNode = NodeFactory.getNode(uri, username);
	                                	if(null != syncUrl && !syncUrl.equals(((ContainerNode)newNode).getNodeSyncTo())) {
	                                		logger.debug("Adding sync region: "+syncRegion+" to "+containerName+" of "+username);
	                                		((ContainerNode)newNode).setNodeSyncTo(syncUrl, syncKey);
	                                	}
                                	} catch(Exception ex) {
                                		ex.printStackTrace();
                                	}
                                }
                            	return true;
                            }
                        }
                );
            }
        };

        cleanerHandle =
            scheduler.scheduleAtFixedRate(cleaner, 1, 1, MINUTES);
    }

    @Override
    public void destroy() {
    	cleanerHandle.cancel(true);
    	scheduler.shutdownNow();
    	System.out.println("Synchronizer is terminating");
    }
        

}


