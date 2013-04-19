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

import java.util.Date;


public class NodeInfo {
	private int revision;
	private boolean isDeleted;
	private Date mtime;
	private long size;
	private String contentType;
	private String chunkedName;

	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	public int getRevision() {
		return revision;
	}
	public Date getMtime() {
		return mtime;
	}
	public long getSize() {
		return size;
	}
	public boolean isDeleted() {
		return isDeleted;
	}
	public void setRevision(int revision) {
		this.revision = revision;
	}
	public void setDeleted(boolean isDeleted) {
		this.isDeleted = isDeleted;
	}
	public void setMtime(Date mtime) {
		this.mtime = mtime;
	}
	public void setSize(long size) {
		this.size = size;
	}
	public String getChunkedName() {
		return chunkedName;
	}
	public void setChunkedName(String chunkedName) {
		this.chunkedName = chunkedName;
	}
}
