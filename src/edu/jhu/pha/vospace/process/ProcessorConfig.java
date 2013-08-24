package edu.jhu.pha.vospace.process;

import java.util.List;
import java.util.Vector;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.apache.tika.sax.BodyContentHandler;
import org.codehaus.jackson.map.ObjectMapper;
import org.xml.sax.ContentHandler;

public class ProcessorConfig {

	private static final Logger logger = Logger.getLogger(ProcessorConfig.class);
	private String id;
	private List<String> mimeTypes = new Vector<String>();
	private String tikaConfig;
	private Class<? extends ContentHandler> handlerClass;
	private Class<? extends Processor> processorClass;
	private String title;
	private CredentialsSchema schema;
	public ProcessorConfig(XMLConfiguration conf, String processorId) {
		this.id = processorId;
		for(String mimeType: conf.getStringArray("//processor[id='"+processorId+"']/mimetype")){
			mimeTypes.add(mimeType);
		}
		this.tikaConfig = conf.getString("//processor[id='"+processorId+"']/config");
		
		String handlerClassStr = conf.getString("//processor[id='"+processorId+"']/handler");
		this.handlerClass = BodyContentHandler.class;
		if(null != handlerClassStr)
			try {
				this.handlerClass = (Class<? extends ContentHandler>)Class.forName(handlerClassStr);
			} catch (Exception ex) {
				logger.error("Error initializing handler class "+handlerClass+": "+ex.getMessage());
				ex.printStackTrace();
			}
		
		String processorClassStr = conf.getString("//processor[id='"+processorId+"']/processor");
		try {
			this.processorClass = (Class<? extends Processor>)Class.forName(processorClassStr);
		} catch (Exception ex) {
			logger.error("Error initializing processor class "+processorClass+": "+ex.getMessage());
			ex.printStackTrace();
		}
		
		this.title = conf.getString("//processor[id='"+processorId+"']/title", processorId);

		String[] namesArray = conf.getStringArray("//processor[id='"+processorId+"']/schema/field/@name");
		CredentialsSchema schema = new CredentialsSchema();
		schema.description = conf.getString("//processor[id='"+processorId+"']/description", "");

		if(namesArray != null && namesArray.length > 0){
			for(String fieldName: namesArray) {
				CredentialsSchemaField field = new CredentialsSchemaField();
				field.setName(fieldName);
				field.setRequired(conf.getBoolean("//processor[id='"+processorId+"']/schema/field[@name = '"+field.getName()+"']/@required"));
				field.setDefaultValue(conf.getString("//processor[id='"+processorId+"']/schema/field[@name = '"+field.getName()+"']/@default"));
				field.setPassword(conf.getBoolean("//processor[id='"+processorId+"']/schema/field[@name = '"+field.getName()+"']/@ispassword", false));
				schema.addField(field);
			}
		}
		this.schema = schema;
	}

	public Class<? extends Processor> getProcessorClass() {
		return processorClass;
	}

	public String getId() {
		return id;
	}
	public boolean isSupportMimeType(String mimeType) {
		return this.mimeTypes.contains(mimeType);
	}
	public String getTikaConfig() {
		return tikaConfig;
	}
	public Class<? extends ContentHandler> getHandlerClass() {
		return handlerClass;
	}
	public String getSchemaAsJson() {
		return schema.toString();
	}
	public String getTitle() {
		return title;
	}
    
    static class CredentialsSchema {
    	private List<CredentialsSchemaField> properties = new Vector<CredentialsSchemaField>();
    	private String description;
    	public void addField(CredentialsSchemaField field) {
    		this.properties.add(field);
    	}
    	public List<CredentialsSchemaField> getFields() {
    		return properties;
    	}
    	@Override
		public String toString() {
			ObjectMapper mapper = new ObjectMapper();
			try {
				return mapper.writeValueAsString(this);
			} catch (Exception e) {
				e.printStackTrace();
				return "{}";
			}
    	}
		public String getDescription() {
			return description;
		}
    }
    
    static class CredentialsSchemaField {
    	private String name;
    	private boolean required;
    	private String defaultValue = "";
    	private boolean isPassword = false;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public boolean isRequired() {
			return required;
		}
		public void setRequired(boolean required) {
			this.required = required;
		}
		public String getDefaultValue() {
			return defaultValue;
		}
		public void setDefaultValue(String defaultValue) {
			this.defaultValue = (null == defaultValue)?"":defaultValue;
		}
		public boolean isPassword() {
			return isPassword;
		}
		public void setPassword(boolean isPassword) {
			this.isPassword = isPassword;
		}
    }

}
