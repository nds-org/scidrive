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

import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.api.exceptions.NotFoundException;
import edu.jhu.pha.vospace.meta.MetaStore;
import edu.jhu.pha.vospace.meta.MetaStoreFactory;
import edu.jhu.pha.vospace.oauth.SciDriveUser;

/** 
 * A factory for creating nodes
 */
public class NodeFactory {
	private NodeFactory() {}

	public static <T extends Node> T createNode(VospaceId uri, SciDriveUser username, NodeType nodeType) {
		T node;
		try {
			node = (T)nodeType.getNodeClass().getConstructor(VospaceId.class, SciDriveUser.class).newInstance(uri, username);
		} catch (Exception e) {
			throw new InternalServerErrorException("InternalFault");
		}
		return node;
	}
	
	public static <T extends Node> T getNode(VospaceId uri, SciDriveUser username) {
		MetaStore metastore = MetaStoreFactory.getMetaStore(username);
		if(null == uri)
			throw new NotFoundException("NodeNotFound");
		NodeType type = metastore.getType(uri);
		return createNode(uri, username, type);
	}
	
}
