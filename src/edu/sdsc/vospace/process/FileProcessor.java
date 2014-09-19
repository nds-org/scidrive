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
package edu.sdsc.vospace.process;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.util.Version;
import org.apache.tika.metadata.MSOffice;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.apache.tika.metadata.Photoshop;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.Photoshop;
import org.apache.tika.metadata.TIFF;
import org.apache.tika.metadata.TikaCoreProperties;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.xml.sax.ContentHandler;

import edu.jhu.pha.vospace.QueueConnector;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.node.DataNode;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.process.ProcessingException;
import edu.jhu.pha.vospace.process.Processor;
import edu.jhu.pha.vospace.process.ProcessorConfig;


public class FileProcessor extends Processor {
	 
	private static Analyzer analysis = new EnglishAnalyzer(Version.LUCENE_41);
	    private static IndexWriterConfig indexconf = new IndexWriterConfig(Version.LUCENE_41, analysis);
		private static Configuration conf = SettingsServlet.getConfig();
		private final static String MIME_TYPE = "ivo://ivoa.net/vospace/core#mime_type";
		private final static String AUTHOR = "ivo://ivoa.net/vospace/core#author";
		private final static String OWNER = "ivo://ivoa.net/vospace/core#owner";
		private final static String CONTENT = "ivo://ivoa.net/vospace/core#contenttype";
		private final static String TITLE = "ivo://ivoa.net/vospace/core#title";
		private final static String FILE_PROCESS = "sdsc.scidrive.FILE";
		private final static String CHARACTER_COUNT = "ivo://ivoa.net/vospace/core#character_count";
	    
		public FileProcessor(ProcessorConfig config) {
			super(config);
		}
		
		@Override
		public void processNodeMeta(Metadata metadata, JsonNode credentials) throws ProcessingException 
		{
	        String owner = metadata.get("owner");
	        String source = metadata.get(TikaCoreProperties.SOURCE);
	        try{

            DataNode node = (DataNode)NodeFactory.getNode(new VospaceId(source), owner);
	        
	        Map<String, String> properties = new HashMap<String, String>();
	        properties.put(AUTHOR, metadata.get(Office.AUTHOR));
	        properties.put(CHARACTER_COUNT, metadata.get(Office.CHARACTER_COUNT));
	        properties.put(TITLE,metadata.get("TITLE"));
	       
	        node.getMetastore().updateUserProperties(node.getUri(), properties);
	        
	       
	        
	        }catch(Exception e){
	        	
	        }	        
	        
	  	}
	
}
