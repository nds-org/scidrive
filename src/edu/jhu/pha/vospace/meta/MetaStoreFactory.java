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

import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;

/** 
 * This class presents a factory for creating MetaStores
 */
public class MetaStoreFactory {

	private static final Logger logger = Logger.getLogger(MetaStoreFactory.class);
	
	private final static Class<? extends MetaStore> metaStoreClass = MySQLMetaStore2.class; 

	private MetaStoreFactory()  {
	}

	public static MetaStore getMetaStore(String username) {
		try {
			return metaStoreClass.getConstructor(String.class).newInstance(username);
		} catch (Exception e) {
			logger.error("Error instantiating metadata store: "+e.getMessage());
			throw new InternalServerErrorException("InternalServerError");
		}
	}
}
