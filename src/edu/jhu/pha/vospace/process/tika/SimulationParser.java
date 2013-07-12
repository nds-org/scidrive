package edu.jhu.pha.vospace.process.tika;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;


public class SimulationParser implements Parser {
	
	private static final long serialVersionUID = -8295762527714045871L;
	
	private static final String MAGIC = "InitialCycleNumber";
	public static final String METADATA_SIMULATION_UUID = "MetaDataSimulationUUID";
	public static final String METADATA_DATASET_UUID = "MetaDataDatasetUUID";
	
	private static Logger logger = Logger.getLogger(SimulationParser.class);
	
	private Set<MediaType> SUPPORTED_TYPES;
	
	public SimulationParser() {
		SUPPORTED_TYPES = new HashSet<MediaType>();
		SUPPORTED_TYPES.add(new MediaType("application","enzosimulation"));
	}
	
	@Override
	public Set<MediaType> getSupportedTypes(ParseContext arg0) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(InputStream stream, ContentHandler handler, Metadata metadata,
			ParseContext context) throws IOException, SAXException, TikaException {

		InputStream is = stream;
		if (! (stream instanceof TikaInputStream)) {
			is = TikaInputStream.get(stream);
		}

		try {
	    	CompressorInputStream cis = new CompressorStreamFactory().createCompressorInputStream(is);
	    	BufferedInputStream bis = new BufferedInputStream(cis);
	    	ArchiveInputStream input = new ArchiveStreamFactory().createArchiveInputStream(bis);
	    	ArchiveEntry entry = null;
	    	do {
	    		entry = input.getNextEntry();
	    		if (entry != null && !entry.isDirectory()) {
	        		input.mark(MAGIC.length()+10);
	        		String s = null;
	    			byte[] magic = new byte[MAGIC.length()];
	    			if (entry.getSize() > magic.length) {
	    				int offset = 0;
	    				int length = magic.length;
	    				while (length > 0) {
	    					int n = input.read(magic, offset, length);
	    					offset += n;
	    					length -= n;
	    				}
	    				s = new String(magic,"ASCII"); 
	    			}
	    			input.reset();
	    			if (MAGIC.equals(s)) {
	    				//System.out.println(entry.getName());
	    				logger.debug("Parsing: "+entry.getName());
	    				BoundedInputStream inp = new BoundedInputStream(input, entry.getSize());
	    				BufferedReader reader = new BufferedReader(new InputStreamReader(inp));
	    				String line = null;
	    				while ((line = reader.readLine()) != null) {
	    					String[] keyValue = line.split("=");
	    					if (keyValue.length == 2) {
	    						String key = keyValue[0].trim();
	    						String value = keyValue[1].trim();
	    						if (METADATA_SIMULATION_UUID.equals(key)) {
	    							metadata.set(METADATA_SIMULATION_UUID, value);
	    						}
	    						if (METADATA_DATASET_UUID.equals(key)) {
	    							metadata.add(METADATA_DATASET_UUID, value);
	    						}
	    					}
	    				}
	    			}
	    		}
	    	} while ((entry != null) /*&& (k < 3)*/);
	    	logger.debug("Simulation UUID: "+metadata.get(METADATA_SIMULATION_UUID)+"; No. of datasets: "+metadata.getValues(METADATA_DATASET_UUID).length);
		}
		catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			throw new TikaException("Error! "+e.getMessage()+"\n"+sw.toString());
		} finally {
	    	try {is.close();} catch(Exception ex) {}
		}
	}
	
	public static void main(String[] args) throws Exception {
		String filename = "C://Projects//Enzo_64.tar.gz";
		Parser parser = new SimulationParser();

		Metadata metadata = new Metadata();
		
		WriteOutContentHandler handler = new WriteOutContentHandler();
		long t1 = Calendar.getInstance().getTimeInMillis();
		parser.parse(new FileInputStream(filename), handler, metadata, new ParseContext());
		long t2 = Calendar.getInstance().getTimeInMillis();
		System.out.println(metadata.toString());
		System.out.println("Time: "+(t2-t1)/1000.0);
	}


}
