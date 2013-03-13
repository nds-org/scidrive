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
package edu.jhu.pha.vospace.jobs;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.node.Node;

/**
 * The servlet runs a jobs processor according to the jobsprocessor.class setting
 * @author Dmitry Mishin, JHU
 *
 */
public class JobsProcessorServlet extends HttpServlet  {

	private static final long serialVersionUID = 8829039248294774005L;
	private static final Logger logger = Logger.getLogger(JobsProcessorServlet.class);
	static Configuration conf = SettingsServlet.getConfig();;
	private static JobsProcessor processor = null;

	static {
		try {
			Class jobsHandlerClass = Class.forName(conf.getString("jobsprocessor.class"));
			processor = (JobsProcessor)jobsHandlerClass.newInstance();
		} catch(ClassNotFoundException e){
			logger.error("Erorr initializing the JobsProcessorServlet: "+e.getMessage());
		} catch (InstantiationException e) {
			logger.error("Erorr initializing the JobsProcessorServlet: "+e.getMessage());
		} catch (IllegalAccessException e) {
			logger.error("Erorr initializing the JobsProcessorServlet: "+e.getMessage());
		}

	}

	@Override
	public void init() throws ServletException {
		String runJobsProc = getServletConfig().getInitParameter("processJobs");
		if(null == runJobsProc || Boolean.parseBoolean(runJobsProc)) {
			processor.start();
		}
	}
	
	@Override
	public void destroy() {
		processor.destroy();
	}

}
