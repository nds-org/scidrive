package edu.sdsc.vospace.process;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.apache.tika.metadata.TikaCoreProperties;
import org.codehaus.jackson.JsonNode;

import edu.jhu.pha.vospace.SettingsServlet;


public class DocumentsProcessor {

    private static Analyzer analysis = new EnglishAnalyzer(Version.LUCENE_41);
    private static IndexWriterConfig indexconf = new IndexWriterConfig(Version.LUCENE_41, analysis);
	private static Configuration conf = SettingsServlet.getConfig();
    
	public static void processNodeMeta(Metadata metadata, Object handler, JsonNode credentials) throws Exception {
        Document doc = new Document();

		Field contents = new TextField("content", handler.toString(), Store.YES);
        doc.add(contents);

        if(null != metadata.get(Office.AUTHOR)) {
	        Field author = new TextField("author", metadata.get(Office.AUTHOR), Store.YES);
        	doc.add(author);
        }

        Field owner = new TextField("owner", metadata.get("owner"), Store.YES);
        doc.add(owner);
        Field source = new TextField("source", metadata.get(TikaCoreProperties.SOURCE), Store.YES);
        doc.add(source);
        
        
        IndexWriter idx = null;
    	try {
    		idx = new IndexWriter(
    				NIOFSDirectory.open(
    						new File(conf.getString("lucene.index")), 
    						new NativeFSLockFactory(conf.getString("lucene.lock"))
    				), 
    				indexconf);
    	} catch(IOException ex) {
    		ex.printStackTrace();
    	}

        idx.addDocument(doc);
        idx.close();
  	}

}

