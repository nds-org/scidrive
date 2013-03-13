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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.configuration.Configuration;

import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;

/** 
 * This class presents a factory for creating StorageManagers
 */
public class StorageManagerFactory {

	private static StorageManagerFactory ref;
	private static Configuration conf = SettingsServlet.getConfig();
	private final String className = conf.getString("filestore.class");
	private Constructor constr;
	
	/* 
	 * Construct a basic StorageManagerFactory: load the properties file 
	 */
	private StorageManagerFactory()  {
		try {
			constr = Class.forName(className).getConstructor(String.class);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Get a StorageManagerFactory
	 */
	public static StorageManagerFactory getInstance() {
		if (ref == null) ref = new StorageManagerFactory();
		return ref;
	}

	/*
	 * Get a StorageManager
	 */
	public StorageManager getStorageManager(String username) {
		try {
			return (StorageManager) constr.newInstance(username);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getMessage());
		} catch (SecurityException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getMessage());
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getMessage());
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getMessage());
		} catch (InvocationTargetException e) {
			e.getCause().printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getCause().getMessage());
		}
	}

	public static String generateRandomCredentials(String username) {
		String className = conf.getString("filestore.class");
		try {
			Method genMethod =Class.forName(className).getMethod("generateRandomCredentials", String.class); 
			if(null != genMethod){
				return (String)genMethod.invoke(null, username);
			}
			return null;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getMessage());
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getMessage());
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getMessage());
		} catch (SecurityException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getMessage());
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getMessage());
		} catch (InvocationTargetException e) {
			e.getCause().printStackTrace();
			throw new InternalServerErrorException("Error initialising class "+className+": "+e.getCause().getMessage());
		}
	}
	
	/*
	 * Prevent cloning
	 */
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}
}
