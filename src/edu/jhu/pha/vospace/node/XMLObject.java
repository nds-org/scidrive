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

import java.util.HashMap;
import java.util.Map;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;

public class XMLObject {
	protected VTDNav vn;
	protected AutoPilot ap;
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
			ap.declareXPathNameSpace("vos", "http://www.ivoa.net/xml/VOSpace/v2.0");
			ap.declareXPathNameSpace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
			ap.declareXPathNameSpace("uws", "http://www.ivoa.net/xml/UWS/v1.0");
			PREFIX = getNamespacePrefix();
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
}
