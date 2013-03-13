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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import no.geosoft.cc.util.SmartTokenizer;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class CSVParser2 implements Parser {
	private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton(MediaType.text("plain"));
	
	private static final Set<Character> POSSIBLE_DELIMITERS = new HashSet<Character>();
	private static final int READ_AHEAD = 100000;
	private static final int MAGIC_LINES = 100;
	
	public CSVParser2() {
		POSSIBLE_DELIMITERS.addAll(Arrays.asList(new Character[] {',',';',' ','\t'}));
	}

	@Override
	public Set<MediaType> getSupportedTypes(ParseContext context) {
		return SUPPORTED_TYPES;	
	}

	@Override
	public void parse(InputStream stream, ContentHandler handler, Metadata metadata,
			ParseContext context) throws IOException, SAXException, TikaException {
		
        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();
        
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		reader.mark(READ_AHEAD);
		
		LineAnalyser magicLines = new LineAnalyser(POSSIBLE_DELIMITERS);
				
		String line = null;
		int linesRead = 0;
		while (linesRead < MAGIC_LINES && (line = reader.readLine())!=null) {
			line = Utils.trimSpaces(line);
			magicLines.addLine(line);
			linesRead++;
		}
		
		magicLines.analyse();
		
		Set<Character> bestDelimiters = magicLines.getBestDelimiters();
		if (bestDelimiters.size() != 1) {
			throw new TikaException("Could not determine best delimiter");
		}
		char bestDelimiter = (Character)bestDelimiters.toArray()[0];
		
		int skipLines = magicLines.getNumHeaderLines(bestDelimiter);
		
		reader.reset();
		
		
		RowAnalyser magicRows = new RowAnalyser(bestDelimiter);
		
		for (int i=0; i<skipLines; i++) {
			reader.readLine();
		}
		
		reader.mark(READ_AHEAD);
		
		linesRead = 0;
		while (linesRead < MAGIC_LINES && (line = reader.readLine()) != null) {
			line = Utils.trimSpaces(line);
			if (!magicLines.isTableLine(line,bestDelimiter)) {
				break;
			}
			magicRows.addRow(line);
			linesRead++;
		}
		
		int headerRows = magicRows.getNumHeaderRows();
		
		reader.reset();
		
		ColumnAnalyser magicCols = new ColumnAnalyser(bestDelimiter, magicLines.getDelimiterCount(bestDelimiter)+1);
		for (int i=0; i<headerRows; i++) {
			line = reader.readLine();
			if (i==0)  {
				line = Utils.trimSpaces(line);
				magicCols.addHeader(line);
			}
		}
		
		linesRead = 0;
		reader.mark(READ_AHEAD);
		while (linesRead < MAGIC_LINES && (line = reader.readLine()) != null) {
			line = Utils.trimSpaces(line);
			if (!magicLines.isTableLine(line,bestDelimiter)) {
				break;
			}
			magicCols.addRow(line); 
			linesRead++;
		}
		
		// Get column names
		List<String> columnNames = magicCols.getColumnNames();
		
		// Determine column types
		List<Integer> columnTypes = magicCols.getColumnTypes();
		
		int nCols = columnNames.size();
		
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
    		String columnType = DataTypes.getCharCode(columnTypes.get(i));
    		xhtml.element("td", columnType);
    	}
		xhtml.endElement("th");
		
		reader.reset();
		while ((line = reader.readLine()) != null) {
			line = Utils.trimSpaces(line);
			if (!magicLines.isTableLine(line,bestDelimiter)) {
				break;
			}
			xhtml.startElement("tr");
			SmartTokenizer tokenizer = new SmartTokenizer(line,String.valueOf(bestDelimiter));
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if ("".equals(token)) token = " ";
				xhtml.element("td",token);
			}
			xhtml.endElement("tr");
		}
			
		xhtml.endElement("table");
		xhtml.endElement("section");
		xhtml.endDocument();
		
		metadata.add(TikaCoreProperties.TYPE, "CSV");
	}
}
