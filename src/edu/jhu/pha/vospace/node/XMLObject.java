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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import com.ximpleware.XMLModifier;

import edu.jhu.pha.vospace.api.exceptions.BadRequestException;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;

public class XMLObject {
	private static final Logger logger = Logger.getLogger(XMLObject.class);
	protected VTDNav vn;
	protected AutoPilot ap;
	private XMLModifier xm;
	protected String PREFIX;

	/**
	 * Construct a XMLObject from the byte array
	 * @param req The byte array containing the Node
	 */
	public XMLObject(byte[] bytes) {
		try {
			VTDGen vg = new VTDGen();
			vg.setDoc(bytes);
			vg.parse(true);
			vn = vg.getNav();
			ap = new AutoPilot();
			xm = new XMLModifier();
			ap.declareXPathNameSpace("vos", "http://www.ivoa.net/xml/VOSpace/v2.0");
			ap.declareXPathNameSpace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
			ap.declareXPathNameSpace("uws", "http://www.ivoa.net/xml/UWS/v1.0");
			PREFIX = getNamespacePrefix();
			if (!validStructure())
				throw new BadRequestException("InvalidArgument");
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	/**
	 * Return the values of the items identified by the specified XPath expression
	 * @param expression The XPath expression identifying the items to retrieve
	 * @return the values of the items identified by the XPath expression
	 */
	public String[] xpath(String expression) {
		try {
			ap.bind(vn);
			ArrayList<String> elements = new ArrayList<String>();
			ap.selectXPath(expression);
			int result = -1;
			while ((result = ap.evalXPath()) != -1) {
				if (vn.getTokenType(result) == VTDNav.TOKEN_ATTR_NAME) {
					elements.add(vn.toNormalizedString(result + 1));
				} else {
					int t = vn.getText();
					if (t > 0) 
						elements.add(vn.toNormalizedString(t));
				}
			}
			ap.resetXPath();
			return elements.toArray(new String[0]);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	public Map<String, String> getNodeProperties() {
		Map<String, String> res = new HashMap<String, String>();
		try {
			ap.bind(vn);
			ap.selectXPath("/vos:node/vos:properties/vos:property");

			int result = -1;
			while ((result = ap.evalXPath()) != -1) {
				if (vn.getTokenType(result) == VTDNav.TOKEN_STARTING_TAG) {
					String uri = vn.toNormalizedString(vn.getAttrVal("uri"));
					
					int nil = vn.getAttrValNS("http://www.w3.org/2001/XMLSchema-instance", "nil");
					if(nil > -1)
						res.put(uri, null);
					else
						res.put(uri, vn.toNormalizedString(vn.getText()));
				}
			}
			ap.resetXPath();
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
		return res;
	}
	

	
	/**
	 * Return the items identified by the specified XPath expression
	 * @param expression The XPath expression identifying the items to retrieve
	 * @return the items identified by the XPath expression as a string
	 */
	public String[] item(String expression) {
		try {
			ArrayList<String> items = new ArrayList<String>();
			ap.bind(vn);
			ap.selectXPath(expression);
			int result = -1;
			while ((result = ap.evalXPath()) != -1) {
				items.add(new String(vn.getElementFragmentNs().toBytes()));
			}
			ap.resetXPath();
			return items.toArray(new String[0]);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	/**
	 * Update the value of the text identified by the XPath expression with the specified string
	 * @param expression The XPath expression identifying the text to be replaced
	 * @param value The new text value 
	 */
	public void replace(String expression, String value) {
		try {
			ap.bind(vn);
			xm.bind(vn);
			ap.selectXPath(expression);
			int result = -1;
			while ((result = ap.evalXPath()) != -1) {
				if (vn.getTokenType(result) == VTDNav.TOKEN_ATTR_NAME) {
					xm.updateToken(result + 1, value);
				} else {
					int t = vn.getText();
					if (t > 0)
						xm.updateToken(t, value);
				}
			}
			vn = xm.outputAndReparse();
			ap.resetXPath();
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	/**
	 * Remove the items identified by the specified XPath expression
	 * @param expression The XPath expression identifying the items to remove
	 */
	public void remove(String expression) {
		try {
			ap.bind(vn);
			xm.bind(vn);
			ap.selectXPath(expression);
			int result = -1;
			while ((result = ap.evalXPath()) != -1) {
				xm.remove();
			}
			vn = xm.outputAndReparse();
			ap.resetXPath();
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	/**
	 * Add the item identified by the specified XPath expression
	 * @param expression The XPath expression identifying where to add the item
	 * @param item The item to add
	 */
	public void add(String expression, String item) {
		try {
			ap.bind(vn);
			xm.bind(vn);
			ap.selectXPath(expression);
			int result = -1;
			while ((result = ap.evalXPath()) != -1) {
				xm.insertAfterElement(item);
			}
			vn = xm.outputAndReparse();
			ap.resetXPath();
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}


	/**
	 * Add the item identified by the specified XPath expression
	 * @param expression The XPath expression identifying where to add the item
	 * @param item The item to add
	 */
	public void addChild(String expression, String item) {
		try {
			ap.bind(vn);
			xm.bind(vn);
			ap.selectXPath(expression);
			int result = -1;
			while ((result = ap.evalXPath()) != -1) {
				xm.insertAfterHead(item);
			}
			vn = xm.outputAndReparse();
			ap.resetXPath();
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	/**
	 * Check whether the specified item exists
	 * @param expression The XPath expression identifying the item to check
	 * @return whether the specified item exists or not
	 */
	public boolean has(String expression) {
		try {
			boolean has = false;
			ap.bind(vn);
			ap.selectXPath(expression);
			if (ap.evalXPath() != -1)
				has = true;
			ap.resetXPath();
			return has;
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	/**
	 * Validate the structure of the document
	 */
	public boolean validStructure() {
		return true;
	}

	/**
	 * Get a byte array corresponding to the object
	 * @return a byte array corresponding to the object
	 */
	public byte[] getBytes() {
		return vn.getXML().getBytes();
	}

	/**
	 * Get the namespace prefix used for the object
	 * @return the namespace prefix used for the object
	 */
	public String getNamespacePrefix() {
		try {
			return vn.getPrefixString(1);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	/**
	 * Get a string representation of the object
	 * @return a string representation of the object
	 */
	public String toString() {
		return new String(this.getBytes()).replaceAll("[\t\n]", "").replaceAll("> +<", "><");
	}
	
	public static void main(String[] s) {
		String st = "<vos:node xmlns:vos=\"http://www.ivoa.net/xml/VOSpace/v2.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"vos:ContainerNode\" uri=\"vos://edu.jhu!vospace/dropcloud/12\" busy=\"false\"><vos:properties></vos:properties><vos:accepts></vos:accepts><vos:provides><vos:view uri=\"ivo://ivoa.net/vospace/views#tap\"/></vos:provides><vos:capabilities></vos:capabilities></vos:node>";
		XMLObject node = new XMLObject(st.getBytes());
		String[] viewStrs = node.item("/vos:node/vos:provides/vos:view");
		for (String viewStr: viewStrs) {
			System.out.println("View: "+viewStr);
		}
		
	}
}
