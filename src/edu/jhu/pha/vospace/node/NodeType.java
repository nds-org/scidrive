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

/**
 * The possible types of node
 */
public enum NodeType  {
    NODE ("Node", Node.class), 
    DATA_NODE ("DataNode", DataNode.class), 
    LINK_NODE ("LinkNode", LinkNode.class), 
    CONTAINER_NODE ("ContainerNode", ContainerNode.class), 
    UNSTRUCTURED_DATA_NODE ("UnstructuredDataNode", UnstructuredDataNode.class), 
    STRUCTURED_DATA_NODE ("StructuredDataNode", StructuredDataNode.class);
    
    private String typeName;
    private Class<? extends Node> nodeClass;

    NodeType(String text, Class<? extends Node> nodeClass) {
      this.typeName = text;
      this.nodeClass = nodeClass;
    }

    public String getTypeName() {
      return this.typeName;
    }
    
    public Class<? extends Node> getNodeClass() {
    	return nodeClass;
    }

    public static NodeType fromString(String text) {
      if (text != null) {
        for (NodeType b : NodeType.values()) {
          if (text.equalsIgnoreCase(b.typeName)) {
            return b;
          }
        }
      }
      return null;
    }
}
