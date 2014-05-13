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
package edu.jhu.pha.vospace.jobs;

import java.net.URISyntaxException;

import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.api.exceptions.BadRequestException;
import edu.jhu.pha.vospace.api.exceptions.ConflictException;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;
import edu.jhu.pha.vospace.meta.MetaStore;
import edu.jhu.pha.vospace.meta.MetaStoreFactory;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.oauth.SciDriveUser;
import edu.jhu.pha.vospace.protocol.ProtocolHandler;
import edu.jhu.pha.vospace.rest.JobDescription;
import edu.jhu.pha.vospace.rest.JobDescription.DIRECTION;
import edu.jhu.pha.vospace.rest.JobDescription.STATE;
import java.util.concurrent.Callable;

public class TransferThread implements Callable<STATE> {

	private static final Logger logger = Logger.getLogger(TransferThread.class);
	
	private final JobDescription job;
	private final JobsProcessor proc;

	public TransferThread(JobDescription job, JobsProcessor proc) {
		this.job = job;
		this.proc = proc;
	}

	/**
	 * Executes the RequestMethod
	 */
	@Override
	public STATE call() {
		JobsProcessor.modifyJobState(job, STATE.RUN);
		logger.debug("Started the job "+job.getId());
		try {
			validateTransfer(job);

			ProtocolHandler handler = null;

			if(job.getDirection().equals(DIRECTION.LOCAL)){ // local job
				copyNode(job, job.isKeepBytes());
				JobsProcessor.modifyJobState(job, STATE.COMPLETED);
			} else {
				for(String protocolKey: job.getProtocols().keySet()) {
					if(null != proc.getProtocolHandler(protocolKey, this)){
						handler = proc.getProtocolHandler(protocolKey, this);
					}
				}

				if(null == handler) {
					JobsProcessor.modifyJobState(job, STATE.ERROR, "The service supports none of the requested Protocols");
				} else {
					handler.invoke(job);
					JobsProcessor.modifyJobState(job, STATE.COMPLETED);
				}
			}
		} catch(InternalServerErrorException ex) {
			ex.printStackTrace();
			JobsProcessor.modifyJobState(job, STATE.ERROR, ex.getResponse().getEntity().toString());
			logger.error("Error executing job "+job.getId()+": "+ex.getResponse().getEntity().toString());
			return STATE.ERROR;
		} catch(BadRequestException ex) {
			ex.printStackTrace();
			JobsProcessor.modifyJobState(job, STATE.ERROR, ex.getResponse().getEntity().toString());
			logger.error("Error executing job "+job.getId()+": "+ex.getResponse().getEntity().toString());
			return STATE.ERROR;
		} catch(Exception ex) {
			ex.printStackTrace();
			JobsProcessor.modifyJobState(job, STATE.ERROR, ex.toString());
			logger.error("Error executing job "+job.getId()+": "+ex.toString());
			return STATE.ERROR;
		}

		synchronized(JobsProcessor.class) {
			JobsProcessor.class.notifyAll();
		}

		return STATE.COMPLETED;
	}

	/**
	 * Copy from the specified target to the direction
	 */
	private static void copyNode(JobDescription transfer, boolean keepBytes) {
		logger.debug("Copying data");
		// Request details
		VospaceId target = transfer.getTargetId();
		VospaceId direction = transfer.getDirectionTargetId();
		// Get node
    	SciDriveUser username = SciDriveUser.fromName(transfer.getUsername());
		Node node = NodeFactory.getNode(target, username);
		
		if (direction.toString().endsWith(".null")) {
			if(!keepBytes)
				node.markRemoved(true);
			// else don't do anything
		} else {
			node.copy(direction, keepBytes);
		}
	}
	
	private static void validateTransfer(JobDescription transfer) {
		/*TODO Check the whole method */
    	SciDriveUser username = SciDriveUser.fromName(transfer.getUsername());

		MetaStore store = MetaStoreFactory.getMetaStore(username);

		// Check transfer details
		VospaceId target = transfer.getTargetId();
		VospaceId direction = transfer.getDirectionTargetId();

		boolean external = !transfer.getDirection().equals(DIRECTION.LOCAL);

		try {
			// Parent node
			if (!external) {
				Node directionParentNode = NodeFactory.getNode(direction.getParent(), username);
				if(!directionParentNode.isStoredMetadata() || !(directionParentNode.getType() == NodeType.CONTAINER_NODE)) 
					throw new BadRequestException("The parent node is not valid.");
			}
		} catch(URISyntaxException ex) {
			throw new BadRequestException("The parent node is not valid.");
		}
		//TODO check logic: when container is removed but not purged and want to create datanode with same name
		// Existence
		if (store.isStored(target)) {
			if (transfer.getDirection().equals(DIRECTION.PUSHTOVOSPACE) || transfer.getDirection().equals(DIRECTION.PULLTOVOSPACE)) {
				// Container
				Node targetNode = NodeFactory.getNode(target, username);

				if (targetNode.getType().equals(NodeType.CONTAINER_NODE)) 
					throw new BadRequestException("Data cannot be uploaded to a container."); 
				
				if(targetNode.getNodeInfo().isDeleted()) {
					targetNode.markRemoved(false);
				}

			}
		} else {
			if (!external) throw new ConflictException("A Node does not exist with the requested URI");
		}
		if (!external && store.isStored(direction)) {
			Node directionNode = NodeFactory.getNode(direction, username);
			 if(!directionNode.getType().equals(NodeType.CONTAINER_NODE) && !store.getNodeInfo(direction).isDeleted())
					 throw new ConflictException("A Node already exists with the requested URI");		
		}
		if (external) {
			// Views
			if (transfer.getViews().isEmpty()) throw new BadRequestException("A View must be specified.");
		}

	}

}
