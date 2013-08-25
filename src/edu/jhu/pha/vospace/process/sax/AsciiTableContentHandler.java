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
package edu.jhu.pha.vospace.process.sax;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class AsciiTableContentHandler implements ContentHandler {

	private enum Element {
		TABLE_ROW,
		COLUMN_NAMES,
		COLUMN_TYPES
	};
	
	private Set<Element> flags = new HashSet<Element>();
	private List<AsciiTable> tables;
	public List<AsciiTable> getTables() {
		return tables;
	}

	private AsciiTable currentTable;
	private String[] currentRow;
	private int currentColumn;
	private int sectionId;
	
	public AsciiTableContentHandler() {
		this.tables = new ArrayList<AsciiTable>();
	}
	
	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (flags.contains(Element.TABLE_ROW)) {
			currentRow[currentColumn] = String.valueOf(ch,start,length);
		}
		else if (flags.contains(Element.COLUMN_TYPES)) {
			currentTable.getColumnTypes()[currentColumn] = String.valueOf(ch,start,length);
		}
		else if (flags.contains(Element.COLUMN_NAMES)) {
			currentTable.getColumnNames()[currentColumn] = String.valueOf(ch,start,length);
		}
 	}

	@Override
	public void endDocument() throws SAXException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {

		if ("tr".equals(localName)) {
			flags.remove(Element.TABLE_ROW);
			currentTable.getRows().add(currentRow);
		}
		
		else if ("th".equals(localName)) {
			flags.remove(Element.COLUMN_TYPES);
			flags.remove(Element.COLUMN_NAMES);
		}
		
		else if ("td".equals(localName)) {
			currentColumn ++;
		}
	}

	@Override
	public void endPrefixMapping(String prefix) throws SAXException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void ignorableWhitespace(char[] ch, int start, int length)
			throws SAXException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processingInstruction(String target, String data)
			throws SAXException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDocumentLocator(Locator locator) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void skippedEntity(String name) throws SAXException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startDocument() throws SAXException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes atts) throws SAXException {
		if ("section".equals(localName)) {
			sectionId = Integer.parseInt(atts.getValue("id"));
		}
		if ("table".equals(localName)) {
			//int tableId = Integer.parseInt(atts.getValue("id"));
			int tableId = sectionId;
			int tableColumns = Integer.parseInt(atts.getValue("columns"));
			currentTable = new AsciiTable(tableId, tableColumns);
			tables.add(currentTable);
		}
		else if ("th".equals(localName)) {
			currentColumn = 0;
			String info = atts.getValue("info");
			if ("columnTypes".equals(info)) {
				flags.add(Element.COLUMN_TYPES);
			}
			else if ("columnNames".equals(info)) {
				flags.add(Element.COLUMN_NAMES);
			}
		}
		else if ("tr".equals(localName)) {
			currentColumn = 0;
			currentRow = new String[currentTable.getColumns()];
			flags.add(Element.TABLE_ROW);
		}
	}

	@Override
	public void startPrefixMapping(String prefix, String uri)
			throws SAXException {
		// TODO Auto-generated method stub
		
	}

}
