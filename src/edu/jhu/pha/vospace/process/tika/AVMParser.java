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
package edu.jhu.pha.vospace.process.tika;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.sanselan.ImageInfo;
import org.apache.sanselan.ImageReadException;
import org.apache.sanselan.Sanselan;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.xpath.XPath;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class AVMParser implements Parser {
	private static final Namespace NAMESPACE_X = Namespace.getNamespace("x", "adobe:ns:meta/");
	private static final Namespace NAMESPACE_RDF = Namespace.getNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
	private static final Namespace NAMESPACE_AVM = Namespace.getNamespace("avm", "http://www.communicatingastronomy.org/avm/1.0/");
	private static final Namespace NAMESPACE_DC = Namespace.getNamespace("dc","http://purl.org/dc/elements/1.1/");
	private static final Namespace NAMESPACE_PHOTOSHOP = Namespace.getNamespace("photoshop","http://ns.adobe.com/photoshop/1.0/");
	private static final int BUFFER_LENGTH = 1024;

	private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton(MediaType.image("tiff"));
	
	public Set<MediaType> getSupportedTypes(ParseContext context) {
		return SUPPORTED_TYPES;	
	}
	
	public void parse(InputStream stream, ContentHandler handler, Metadata metadata,
			ParseContext context) throws IOException, SAXException, TikaException {
		
        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();
        
        String prefix = "section0:";
        
        ByteArrayOutputStream temp = new ByteArrayOutputStream();
        byte[] buffer = new byte[BUFFER_LENGTH];
        int len = 0;
        while ((len = stream.read(buffer))>0) {
        	temp.write(buffer, 0, len);
        }
        
        byte[] bytes = temp.toByteArray();
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        
        try {
    		ImageInfo info = Sanselan.getImageInfo(is,null);
    		int bitPix = info.getBitsPerPixel();
    		int nAxis1 = info.getWidth();
    		int nAxis2 = info.getHeight();
    		
    		metadata.add(prefix+"BITPIX", String.valueOf(bitPix));
    		metadata.add(prefix+"NAXIS1", String.valueOf(nAxis1));
    		metadata.add(prefix+"NAXIS2", String.valueOf(nAxis2));
    		
    		is.reset();
        	String xmp = Sanselan.getXmpXml(is,null);
    		SAXBuilder builder = new SAXBuilder();
    		StringReader reader = new StringReader(xmp);
    		Document doc = builder.build((Reader)reader);
    		
    		XPath xpath;
    		List<Element> elem;
    		
    		xpath = XPath.newInstance("/x:xmpmeta/rdf:RDF/rdf:Description/avm:Spatial.ReferencePixel/rdf:Seq");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_AVM);
    		elem = extractChildrenOrSelf(doc,xpath);
    		if (elem != null) {
    			metadata.add(prefix+"CRPIX1", (elem.get(0)).getValue());
    			metadata.add(prefix+"CRPIX2", (elem.get(1)).getValue());
    		}

    		xpath = XPath.newInstance("/x:xmpmeta/rdf:RDF/rdf:Description/avm:Spatial.ReferenceValue/rdf:Seq");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_AVM);		
    		elem = extractChildrenOrSelf(doc,xpath);
    		if (elem != null) {
    			metadata.add(prefix+"CRVAL1", (elem.get(0)).getValue());
    	    	metadata.add(prefix+"CRVAL2", (elem.get(1)).getValue());
    		}
    		
    		xpath = XPath.newInstance("/x:xmpmeta/rdf:RDF/rdf:Description/avm:Spatial.CoordsystemProjection");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_AVM);		
    		elem = extractChildrenOrSelf(doc,xpath);
    		if (elem != null) {
    			metadata.add(prefix+"CTYPE1", (elem.get(0)).getValue());
    	    	metadata.add(prefix+"CTYPE2", (elem.get(0)).getValue());
    		}
    			
    		xpath = XPath.newInstance("/x:xmpmeta/rdf:RDF/rdf:Description/avm:Temporal.IntegrationTime/rdf:Seq");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_AVM);
    		elem = extractChildrenOrSelf(doc,xpath);
    		if (elem != null) metadata.add(prefix+"EXPTIME", (elem.get(0)).getValue());
    		
    		xpath = XPath.newInstance("/x:xmpmeta/rdf:RDF/rdf:Description/dc:subject/rdf:Bag");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_DC);
    		elem = extractChildrenOrSelf(doc,xpath);
    		if (elem != null) metadata.add(prefix+"OBJECT", (elem.get(0)).getValue());
    		
    		xpath = XPath.newInstance("/x:xmpmeta/rdf:RDF/rdf:Description/avm:Facility/rdf:Seq");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_AVM);
    		elem = extractChildrenOrSelf(doc,xpath);
    		if (elem != null) metadata.add(prefix+"TELESCOP", (elem.get(0)).getValue());
    		
    		xpath = XPath.newInstance("x:xmpmeta/rdf:RDF/rdf:Description/avm:Temporal.StartTime/rdf:Seq");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_AVM);
    		elem = extractChildrenOrSelf(doc,xpath);
	    	if (elem != null) metadata.add(prefix+"DATEOBS", (elem.get(0)).getValue());
	    	
    		xpath = XPath.newInstance("x:xmpmeta/rdf:RDF/rdf:Description/photoshop:DateCreated");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_PHOTOSHOP);
    		elem = extractChildrenOrSelf(doc,xpath);
    		if (elem != null) metadata.add(prefix+"DATE", (elem.get(0)).getValue());
    		
    		xpath = XPath.newInstance("x:xmpmeta/rdf:RDF/rdf:Description/photoshop:Source");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_PHOTOSHOP);
    		elem = extractChildrenOrSelf(doc,xpath);
    		if (elem != null) metadata.add(prefix+"ORIGIN", (elem.get(0)).getValue());
    		
    		xpath = XPath.newInstance("/x:xmpmeta/rdf:RDF/rdf:Description/avm:Spatial.Scale/rdf:Seq");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_AVM);		
    		List<Element> scale = extractChildrenOrSelf(doc,xpath);
    		xpath = XPath.newInstance("/x:xmpmeta/rdf:RDF/rdf:Description/avm:Spatial.Rotation");
    		xpath.addNamespace(NAMESPACE_X);
    		xpath.addNamespace(NAMESPACE_RDF);
    		xpath.addNamespace(NAMESPACE_AVM);		    		
    		List<Element> rotation = extractChildrenOrSelf(doc,xpath);
    		if ((scale != null) && (rotation != null)) {
    			double cdelt1 = Double.parseDouble(scale.get(0).getValue());
    			double cdelt2 = Double.parseDouble(scale.get(1).getValue());
    			double crota = Double.parseDouble(rotation.get(0).getValue());
    			crota = crota*Math.PI/180.0;
    			double cd1_1 = Math.cos(crota)*cdelt1;
    			double cd1_2 = -Math.sin(crota)*cdelt1;
    			double cd2_1 = Math.sin(crota)*cdelt2;
    			double cd2_2 = Math.cos(crota)*cdelt2;
    			metadata.add(prefix+"CD1_1", String.valueOf(cd1_1));
    	    	metadata.add(prefix+"CD1_2", String.valueOf(cd1_2));
    			metadata.add(prefix+"CD2_1", String.valueOf(cd2_1));
    	    	metadata.add(prefix+"CD2_2", String.valueOf(cd2_2));
    		}

    		metadata.set(TikaCoreProperties.TYPE,"TIFF:IMAGE");

        }
        catch (ImageReadException e) {
        	throw new TikaException("Error reading image",e);
        }
        catch (JDOMException e) {
        	throw new TikaException("Error parsing XMP metadata",e);
        }
	}
	
	private List<Element> extractChildrenOrSelf(Document doc, XPath xpath) throws JDOMException {
		List<Element> result = null;
		Element elem = (Element)xpath.selectSingleNode(doc);
		if (elem != null) {
			result = (List<Element>)elem.getChildren();
			if (result == null || result.isEmpty()) {
				result = new ArrayList<Element>();
				result.add(elem);
			}
		}
		return result;
	}
}
