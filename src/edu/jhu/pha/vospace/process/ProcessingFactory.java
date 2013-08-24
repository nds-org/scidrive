package edu.jhu.pha.vospace.process;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodePath;

public class ProcessingFactory {

	private static final Logger logger = Logger.getLogger(ProcessingFactory.class);
	private final static Map<String, ProcessorConfig> processorConfigs = new HashMap<String, ProcessorConfig>();

	private static final ProcessingFactory instance = new ProcessingFactory();
	
	static {

		try	{
			XMLConfiguration processorConf = new XMLConfiguration("processors.xml");
			processorConf.setExpressionEngine(new XPathExpressionEngine());

			for(String processorId: processorConf.getStringArray("//processor/id")) {
				processorConfigs.put(processorId, new ProcessorConfig(processorConf, processorId));
			}
		} catch(ConfigurationException ex) {
		    logger.error("Error reading the nodes processor configuration file processors.xml." + ex.getMessage());
		}
	}

	private ProcessingFactory() {}
	
	public static ProcessingFactory getInstance() {
		return instance;
	}

	public Map<String, ProcessorConfig> getProcessorConfigs() {
		return processorConfigs;
	}
		
	public List<ProcessorConfig> getProcessorConfigsForNode(Node node, JsonNode allCredentials) {
		List<ProcessorConfig> processorsResult = new ArrayList<ProcessorConfig>();
		
		for(Iterator<String> processorIds = allCredentials.getFieldNames(); processorIds.hasNext();) {
			String processorId = processorIds.next();
			ProcessorConfig processorConf = processorConfigs.get(processorId);
			JsonNode credentialsNode = allCredentials.findValue(processorId);
			
			String nodeContainer = node.getUri().getNodePath().getContainerName();
			
			JsonNode containersNode = credentialsNode.get("containers");
			boolean parseContainer = false;
			if(null != containersNode) {
				for(JsonNode curContainer: containersNode) {
					NodePath containerPath = new NodePath(curContainer.getTextValue());
					if(containerPath.getContainerName().equals(nodeContainer)) {
						parseContainer = true;
					}
				}
			}
			
			if(null != processorConf
					&& parseContainer
					&& processorConf.isSupportMimeType(node.getNodeInfo().getContentType())) {
				processorsResult.add(processorConf);
			}
		}
		logger.debug("Returning processors: "+processorsResult.size());
		return processorsResult;
	}

}
