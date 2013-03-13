/*
 * See COPYING for license information.
 */ 

package com.rackspacecloud.client.cloudfiles;

/**
 * Contains basic information about the container
 * 
 * @author lvaughn
 *
 */
public class FilesContainerInfo
{
    private int objectCount;
    private long totalSize;
    private String name;
    private String syncTo;

    /**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
     * @param containerCount The number of objects in the container
     * @param totalSize      The total size of the container (in bytes)
     */
    FilesContainerInfo(String name, int containerCount, long totalSize, String syncTo)
    {
    	this.name = name;
        this.objectCount = containerCount;
        this.totalSize = totalSize;
        this.syncTo = syncTo;
    }

    public String getSyncTo() {
		return syncTo;
	}

	/**
     * Returns the number of objects in the container
     * 
     * @return The number of objects
     */
    public int getObjectCount()
    {
        return objectCount;
    }

    /**
     * @return The total size of the objects in the container (in bytes)
     */
    public long getTotalSize()
    {
    	return totalSize;
    }

}
