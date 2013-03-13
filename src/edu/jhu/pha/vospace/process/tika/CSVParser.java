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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class CSVParser implements Parser {
	private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton(MediaType.text("plain"));
	private static final String DELIMITER = ",";
	private static final String[] TYPE_CODES = new String[] {"B","I","J","K","E","D","A"};
	private static final int TYPE_BYTE = 0;
	private static final int TYPE_SHORT = 1;
	private static final int TYPE_INT = 2;
	private static final int TYPE_LONG = 3;
	private static final int TYPE_SINGLE = 4;
	private static final int TYPE_DOUBLE = 5;
	private static final int TYPE_STRING = 6;

	public Set<MediaType> getSupportedTypes(ParseContext context) {
		return SUPPORTED_TYPES;	
	}

	public void parse(InputStream stream, ContentHandler handler, Metadata metadata,
			ParseContext context) throws IOException, SAXException, TikaException {
		
        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();
        
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		List<String> lines = new ArrayList<String>();
		String line = null;
		while ((line = reader.readLine())!=null) {
			lines.add(line);
		}
		
		int nRows = lines.size() - 1;
		
		StringTokenizer tokenizer;
		// Get column names
		List<String> columnNames = new ArrayList<String>();
		String header = lines.get(0);
		tokenizer = new StringTokenizer(header,DELIMITER);
		while (tokenizer.hasMoreTokens()) {
			String column = tokenizer.nextToken();
			columnNames.add(column);
		}
		int nCols = columnNames.size();
		
		// Determine column types
		int[] columnTypes = new int[columnNames.size()];
		Arrays.fill(columnTypes, 0);
		
		for (int i=1; i<=nRows; i++) {
			String s = lines.get(i);
			tokenizer = new StringTokenizer(s,DELIMITER);
			int n = 0;
			while (tokenizer.hasMoreTokens()) {
				if (n >= nCols) {
					throw new TikaException("The number of data columns does not match the header");
				}
				String value = tokenizer.nextToken();
				columnTypes[n] = Math.max(columnTypes[n], getTypeCode(value));
				n++;
			}
			if (n < nCols) {
				throw new TikaException("The number of data columns does not match the header");
			}
		}
		
		// Parse table
		xhtml.startElement("section","id",String.valueOf(1));
		xhtml.startElement("table","columns",String.valueOf(nCols));

		xhtml.startElement("th", "info", "columnNames");
		for (int i=0; i<nCols; i++) {
    		xhtml.element("td", columnNames.get(i));
    	}
		xhtml.endElement("th");

		xhtml.startElement("th", "info", "columnTypes");
		for (int i=0; i<nCols; i++) {
    		String columnType = TYPE_CODES[columnTypes[i]];
    		xhtml.element("td", columnType);
    	}
		xhtml.endElement("th");
		
		for (int i=1; i<=nRows; i++) {
			xhtml.startElement("tr");
			String s = lines.get(i);
			tokenizer = new StringTokenizer(s,DELIMITER);
			while (tokenizer.hasMoreTokens()) {
				xhtml.element("td",tokenizer.nextToken());
			}
			xhtml.endElement("tr");
		}
			
		xhtml.endElement("table");
		xhtml.endElement("section");
		xhtml.endDocument();
		
		metadata.add(TikaCoreProperties.TYPE, "CSV");
	}
	
	private int getTypeCode(String value) {
		try {
			byte v = Byte.parseByte(value);
			if (v > 0) return TYPE_BYTE;
			else return TYPE_SHORT;
		}
		catch (Exception e) {}
		
		try {
			short v = Short.parseShort(value);
			return TYPE_SHORT;
		}
		catch (Exception e) {}
		
		try {
			int v = Integer.parseInt(value);
			return TYPE_INT;
		}
		catch (Exception e) {}
		
		try {
			long v = Long.parseLong(value);
			return TYPE_LONG;
		}
		catch (Exception e) {}
		
		try {
			float v = Float.parseFloat(value);
			return TYPE_SINGLE;
		}
		catch (Exception e) {}
		
		try {
			double v = Double.parseDouble(value);
			return TYPE_DOUBLE;
		}
		catch (Exception e) {}
		
		return TYPE_STRING;
	}

}
