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
package edu.jhu.pha.vospace.node;

import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.meta.MetaStore;
import edu.jhu.pha.vospace.meta.MetaStoreFactory;

/** 
 * A factory for creating nodes
 */
public class NodeFactory {
	private static NodeFactory ref;
	private static final Logger logger = Logger.getLogger(NodeFactory.class);
	private NodeFactory() {}

	/*
	 * Get a NodeFactory
	 */
	public static NodeFactory getInstance() {
		if (null == ref) ref = new NodeFactory();
		return ref;
	}
	
	public static Node createNode(VospaceId uri, String username, NodeType type) {
		Node node = null;
		try {
			Class nodeClass = type.getNodeClass();
			node = (Node) nodeClass.getConstructor(VospaceId.class, String.class).newInstance(uri, username);
		} catch (Exception e) {
			throw new InternalServerErrorException("InternalFault");
		}
		return node;
	}
	
	public static Node getNode(VospaceId uri, String username) {
		MetaStore metastore = MetaStoreFactory.getInstance().getMetaStore(username);
		if(null == uri)
			throw new NotFoundException("NodeNotFound");
		NodeType type = metastore.getType(uri);
		return createNode(uri, username, type);
	}
	
}
