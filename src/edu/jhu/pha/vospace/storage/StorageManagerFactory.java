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
package edu.jhu.pha.vospace.storage;

import java.lang.reflect.Method;

import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;

/** 
 * This class presents a factory for creating StorageManagers
 */
public class StorageManagerFactory {

	private static Class<? extends StorageManager> storageClass = SwiftStorageManager.class;
	
	private StorageManagerFactory()  {
	}

	/*
	 * Get a StorageManager
	 */
	public static StorageManager getStorageManager(String username) {
		try {
			return storageClass.getConstructor(String.class).newInstance(username);
		} catch (Exception e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+e.getMessage());
		}
	}

	public static String generateRandomCredentials(String username) {
		try {
			Method genMethod =storageClass.getMethod("generateRandomCredentials", String.class); 
			if(null != genMethod){
				return (String)genMethod.invoke(null, username);
			}
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+e.getMessage());
		}
	}
}
