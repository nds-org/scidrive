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
package edu.jhu.pha.vospace.rest;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.codehaus.jackson.annotate.JsonIgnore;

import edu.jhu.pha.vospace.node.VospaceId;

/**
 * Transfer job description java bean
 * @author Dmitry Mishin
 */
public class JobDescription implements Serializable {
	private static final long serialVersionUID = 4846684191233497876L;

	/** The job states enumeration */
	public enum STATE { PENDING, RUN, ERROR, COMPLETED};

	/** The job directions enumeration */
	public enum DIRECTION { PULLTOVOSPACE, PULLFROMVOSPACE, PUSHTOVOSPACE, PUSHFROMVOSPACE, LOCAL};
	
	/** The job target **/
	private VospaceId target;

	/** The job direction **/
	private DIRECTION direction;

	/** The local direction target**/
	private VospaceId directionTarget;

	/** The job views **/
	private ArrayList<String> views = new ArrayList<String>();

	/** The job protocols **/
	private HashMap<String, String> protocols = new HashMap<String, String>();

	/** The job identifier **/
	private String id;

	/** The job start time **/
	private Date startTime;

	/** The job end time **/
	private Date endTime;

	/** The job state **/
	private STATE state = STATE.PENDING;
	
	/** The job owner username **/
	private String username;

	/** The job note (error message) **/
	private String note;
	
	private boolean keepBytes;

	public boolean isKeepBytes() {
		return keepBytes;
	}
	public void setKeepBytes(boolean keepBytes) {
		this.keepBytes = keepBytes;
	}
	public String getDirectionTarget() {
		if(null == directionTarget)
			return null;
		return directionTarget.toString();
	}
	@JsonIgnore
	public VospaceId getDirectionTargetId() {
		return directionTarget;
	}
	public void setDirectionTarget(String directionTarget) throws URISyntaxException {
		if(null == directionTarget)
			this.directionTarget = null;
		else
			this.directionTarget = new VospaceId(directionTarget);
	}
	public String getNote() {
		return note;
	}
	public void setNote(String note) {
		this.note = note;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getTarget() {
		if(null == target)
			return null;
		return target.toString();
	}
	@JsonIgnore
	public VospaceId getTargetId() {
		return target;
	}
	public void setTarget(String target) throws URISyntaxException {
		if(null == target)
			this.target = null;
		else
			this.target = new VospaceId(target);
	}
	public DIRECTION getDirection() {
		return direction;
	}
	public void setDirection(DIRECTION direction) {
		this.direction = direction;
	}
	public ArrayList<String> getViews() {
		return views;
	}
	public void addView(String view) {
		views.add(view);
	}
	public void addProtocol(String protocol, String endpoint) {
		if(null == endpoint) endpoint = "";
		this.protocols.put(protocol, endpoint);
	}
	public HashMap<String, String> getProtocols() {
		return protocols;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Date getStartTime() {
		return startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	public Date getEndTime() {
		return endTime;
	}
	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}
	public JobDescription.STATE getState() {
		return state;
	}
	public void setState(JobDescription.STATE state) {
		this.state = state;
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("target: "+getTarget()+"; ");
		buf.append("direction: "+getDirection()+"; ");
		for(String prot: getProtocols().keySet()){
			buf.append("protocol: "+prot+", "+getProtocols().get(prot)+"; ");
		}
		buf.append("direction: "+getDirection()+"; ");
		return buf.toString();
	}

}
