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

import java.util.List;

import edu.jhu.pha.vospace.node.Node;

/**
 * The class provides container for paginated nodes list containing current page of nodes and total amount of results found in the DB
 * @author dmitry
 *
 */
public class NodesList {
	private List<Node> nodesList;
	private int nodesCount;
	
	public NodesList(List<Node> nodesList, int nodesCount) {
		super();
		this.nodesList = nodesList;
		this.nodesCount = nodesCount;
	}
	public List<Node> getNodesList() {
		return nodesList;
	}
	public void setNodesList(List<Node> nodesList) {
		this.nodesList = nodesList;
	}
	public int getNodesCount() {
		return nodesCount;
	}
	public void setNodesCount(int nodesCount) {
		this.nodesCount = nodesCount;
	}

}
