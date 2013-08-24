package edu.jhu.pha.vospace.process;

import org.apache.tika.metadata.Metadata;
import org.codehaus.jackson.JsonNode;
import org.xml.sax.ContentHandler;

public abstract class Processor {
	
	protected ContentHandler handler;
	
	protected Processor(ProcessorConfig processorConfig) {
		try {
			this.handler = processorConfig.getHandlerClass().getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}
	
	public static Processor fromProcessorConfig(ProcessorConfig processorConfig) {
		try {
			Processor processor = (Processor)processorConfig.getProcessorClass().getDeclaredConstructor(ProcessorConfig.class).newInstance(processorConfig);
			return processor;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	public ContentHandler getContentHandler() {
		return this.handler;
	}
	
	public abstract void processNodeMeta(Metadata metadata, JsonNode credentials) throws ProcessingException;
}
