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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.util.TokenBuffer;

import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;

public class StructuredDataNode extends DataNode {

	private static final Logger logger = Logger.getLogger(StructuredDataNode.class);
	private static final MappingJsonFactory f = new MappingJsonFactory();

	public StructuredDataNode(VospaceId id, String username) {
		super(id, username);
	}

    /**
     * Construct a Node from the byte array
     * @param req The byte array containing the Node
     */
	public StructuredDataNode(byte[] bytes, String username, VospaceId id)  {
        super(bytes, username, id);
    }

	@Override
	public NodeType getType() {
		return NodeType.STRUCTURED_DATA_NODE;
	}

	@Override
	public Object export(String format, Detail detail, boolean includeDeleted) {
		if(format.equals("json-dropbox") || format.equals("json-dropbox-object")){
			TokenBuffer g = new TokenBuffer(null);
			try {
	        	g.writeStartObject();

				g.writeStringField("size", readableFileSize(getNodeInfo().getSize()));
				g.writeNumberField("rev", getNodeInfo().getRevision());
				g.writeBooleanField("thumb_exists", false);
				g.writeNumberField("bytes", getNodeInfo().getSize());
				g.writeStringField("modified", dropboxDateFormat.format(getNodeInfo().getMtime()));
				g.writeStringField("path", getUri().getNodePath().getNodeOuterPath());
				g.writeBooleanField("is_dir", false);
				if(includeDeleted || getNodeInfo().isDeleted())
					g.writeBooleanField("is_deleted", getNodeInfo().isDeleted());
				g.writeStringField("icon", "table");
				g.writeStringField("root", (getUri().getNodePath().isEnableAppContainer()?"sandbox":"dropbox"));
				g.writeStringField("mime_type", getNodeInfo().getContentType());
	        	
	        	g.writeEndObject();
		    	g.close(); // important: will force flushing of output, close underlying output stream
			} catch (JsonGenerationException e) {
				e.printStackTrace();
				throw new InternalServerErrorException("Error generationg JSON: "+e.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
				throw new InternalServerErrorException("Error generationg JSON: "+e.getMessage());
			}
			
			if(format.equals("json-dropbox")) {
				try {
			    	ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
					JsonGenerator g2 = f.createJsonGenerator(byteOut); //.useDefaultPrettyPrinter() - doesn't work with metadata header
					g.serialize(g2);
					g2.close();
					byteOut.close();
	
					return byteOut.toByteArray();
				} catch (IOException e1) { // shouldn't happen
					logger.error("Error creating JSON generator: "+e1.getMessage());
					throw new InternalServerErrorException("Error creating JSON generator: "+e1.getMessage());
				}

			} else {
				try {
					return g.asParser(f.getCodec()).readValueAsTree();
				} catch (JsonProcessingException e) {
					logger.error("Error generating JSON: "+e.getMessage());
					throw new InternalServerErrorException("Error generating JSON: "+e.getMessage());
				} catch (IOException e) {
					logger.error("Error generating JSON: "+e.getMessage());
					throw new InternalServerErrorException("Error generating JSON: "+e.getMessage());
				}
			}
		} else {
			return getXmlMetadata(detail).getBytes();
		}
	}


}
