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

import java.io.InputStream;
import edu.jhu.pha.vospace.node.NodeInfo;
import edu.jhu.pha.vospace.node.NodePath;

/**
 * Interface for communicating with backend storage
 */
public interface StorageManager {


    /**
     * Copy the bytes from the specified old location to the specified new location
     * in the current backend storage
     * @param oldLocationId The old location of the bytes
     * @param newLocationId The new location of the bytes
     * @param keepBytes remove the old object
     */
    public void copyBytes(NodePath oldNodePath, NodePath newNodePath, boolean keepBytes);

    /**
     * Create a container at the specified location in the current backend storage
     * @param locationId The location of the container
     */
    public void createContainer(NodePath nodePath);

    /**
     * Get the bytes from the specified location in the current backend storage
     * @param locationId The location of the bytes
     * @return a stream containing the requested bytes
     */
    public InputStream getBytes(NodePath nodePath);

    /**
     * Returns the SWIFT storage URL for current account
     * @return
     */
    public String getStorageUrl();
    
    public long getBytesUsed();
    
    /**
     * Returns the metadata record of container syncto address in SWIFT storage
     * @param container
     * @return
     */
    public String getNodeSyncAddress(String container);
	
	/**
     * Put the bytes from the specified input stream at the specified location in 
     * the current backend storage
     * @param nodePath the node path
     * @param stream The stream containing the bytes
     */
    public void putBytes(NodePath nodePath, InputStream stream);

	/**
     * Create a manifest the uploaded chunks
     * @param locationId The location for the bytes
     * @param chunkedId the name of chunks folder
     */
    public void putChunkedBytes(NodePath nodePath, String chunkedId);

	/**
     * Remove the bytes at the specified location in the current backend storage
	 * @param nodePath
	 * @param removeChunks
	 */
    public void remove(NodePath nodePath, boolean removeChunks);
    
    public void setNodeSyncTo(String container, String syncTo, String syncKey); 

    /**
     * Update node metadata object from data in storage
     * @param nodePath Node location
     * @param nodeInfo The metadata object to update
     */
	public void updateNodeInfo(NodePath nodePath, NodeInfo nodeInfo);

}
