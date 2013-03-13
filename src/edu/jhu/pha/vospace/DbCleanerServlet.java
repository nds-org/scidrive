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

import static java.util.concurrent.TimeUnit.*;
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
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.VospaceId;

public class DbCleanerServlet extends HttpServlet {
	
	private static final long serialVersionUID = -6837095401346471188L;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private ScheduledFuture<?> cleanerHandle;
    
    @Override
	public void init() {
        final Runnable cleaner = new Runnable() {
            public void run() {
                DbPoolServlet.goSql("Cleaning DB nodes",
                		"select identifier, owner from nodes where deleted = 1 and mtime < (NOW() - INTERVAL 5 MINUTE)",
                        new SqlWorker<Boolean>() {
                            @Override
                            public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
                                ResultSet resSet = stmt.executeQuery();
                                while(resSet.next()) {
                                	String uriStr = resSet.getString(1);
                                	String username = resSet.getString(2);
                                	
                                	try {
	                                	VospaceId uri = new VospaceId(uriStr);
	                                	
	                                	Node newNode = NodeFactory.getInstance().getNode(uri, username);
	                                	newNode.remove();
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
            scheduler.scheduleAtFixedRate(cleaner, 1, 5, MINUTES);
    }

    @Override
    public void destroy() {
    	cleanerHandle.cancel(true);
    	scheduler.shutdownNow();
    	System.out.println("Cleaner is terminating");
    }
        

}


