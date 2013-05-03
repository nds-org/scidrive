package org.apache.tika.parser.pkg;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.BufferedInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;

public class SimulationDetector implements Detector {

	private static final long serialVersionUID = 4245177957258498279L;

	private static final String MAGIC = "InitialCycleNumber";
    
	public MediaType detect(InputStream input, Metadata metadata)
            throws IOException {

        if (input == null) {
            return MediaType.OCTET_STREAM;
        }

        TemporaryResources tmp = new TemporaryResources();
        
        try {
            TikaInputStream tis = TikaInputStream.get(input, tmp);            
            MediaType type = detectSimulation(tis); 
            return type;
        }
        finally {
            try {
                tmp.dispose();
            } catch (TikaException e) {
                // ignore
            }
        }
    }

    private static MediaType detectSimulation(TikaInputStream tis) {
    	try {
	    	CompressorInputStream cis = new CompressorStreamFactory().createCompressorInputStream(tis);
	    	BufferedInputStream bis = new BufferedInputStream(cis);
	    	ArchiveInputStream input = new ArchiveStreamFactory().createArchiveInputStream(bis);
	    	ArchiveEntry entry = null;
	    	do {
	    		entry = input.getNextEntry();
	    		//input.mark(MAGIC.length()+10);
	    		if (!entry.isDirectory()) {
	    			byte[] content = new byte[MAGIC.length()];
	    			if (entry.getSize() > content.length) {
	    				int offset = 0;
	    				int length = content.length;
	    				while (length > 0) {
	    					int n = input.read(content, offset, length);
	    					offset += n;
	    					length -= n;
	    				}
	    				String s = new String(content,"ASCII");
	    				if (MAGIC.equals(s)) return new MediaType("application","enzosimulation");
	    			}
	    		}
	    		//input.reset();
	    	} while (entry != null);
	    	
	    	return null;
    	}
    	catch (Exception e) {
    		return null;
    	}
    }
}
