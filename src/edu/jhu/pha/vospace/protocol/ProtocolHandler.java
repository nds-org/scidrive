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
package edu.jhu.pha.vospace.protocol;

import edu.jhu.pha.vospace.rest.JobDescription;

/**
 * This interface represents the implementation details of a protocol
 * involved in a data transfer
 */
public interface ProtocolHandler {

    /**
     * Return the registered identifier for this protocol 
     * @return
     */
    public String getUri();

    /**
     * Invoke the protocol handler and transfer data
     * @param job
     * @return
     */
    public void invoke(JobDescription job) throws Exception; 
}
