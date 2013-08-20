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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.servlet.http.HttpServlet;

import org.apache.log4j.Logger;

public class NodeProcessingServlet extends HttpServlet {

	private static final long serialVersionUID = -2827132663634842769L;
    private static final Logger logger = Logger.getLogger(NodeProcessingServlet.class);

	ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
	public void init() {
    	Future npFuture = executor.submit(new NodeProcessor());
    }

	@Override
	public void destroy() {
		executor.shutdownNow();
	}
}


