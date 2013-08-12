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
package edu.jhu.pha.vospace.process;

import javax.servlet.http.HttpServlet;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.SettingsServlet;

public class NodeProcessingServlet extends HttpServlet {

	private static final long serialVersionUID = -2827132663634842769L;
    private static final Logger logger = Logger.getLogger(NodeProcessingServlet.class);

    private Thread nodeProcThread = null;

    static Configuration conf = SettingsServlet.getConfig();

    @Override
	public void init() {
		nodeProcThread = new NodeProcessor();
		nodeProcThread.setDaemon(true);
		String runProc = getServletConfig().getInitParameter("processNodes");
		if(null == runProc || Boolean.parseBoolean(runProc)) {
			nodeProcThread.start();
		}
    }

	@Override
	public void destroy() {
		logger.debug("INTERRUPTING nodeProcessor!");
		nodeProcThread.interrupt();
	}
}


