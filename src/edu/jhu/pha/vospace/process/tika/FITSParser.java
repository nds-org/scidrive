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
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;
import nom.tam.fits.HeaderCard;
import nom.tam.fits.TableHDU;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;



public class FITSParser implements Parser {

	private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton(MediaType.application("fits"));
	public static final String FITS_MIME_TYPE = "application/fits";
	public static final String FITS_TABLE_HDU_NUMBERS = "fits:TableHduNumbers";
	
	@Override
	public Set<MediaType> getSupportedTypes(ParseContext arg0) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(InputStream stream, ContentHandler handler, Metadata metadata,
			ParseContext context) throws IOException, SAXException, TikaException {
        
		metadata.set(Metadata.CONTENT_TYPE, FITS_MIME_TYPE);
		
        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();
        
		try {
			Fits fits = new Fits(stream);
			BasicHDU hdu = null;
			int i = 0;
			while((hdu=fits.readHDU())!=null) {
				String prefix = "section"+i+":";
				/*
				xhtml.startElement("section","class","hdu");
				xhtml.element("h1","HDU No."+i);
				xhtml.startElement("section","class","header");
				xhtml.element("h2","Header");
				xhtml.startElement("table");
				xhtml.startElement("tr");
				xhtml.element("th", "Keyword");
				xhtml.element("th", "Value");
				xhtml.element("th", "Comment");
				xhtml.endElement("tr");
				*/
				Header header = hdu.getHeader();
				Iterator<HeaderCard> it = header.iterator();
				while (it.hasNext()) {
					HeaderCard card = it.next();
					if (card.isKeyValuePair()) {
						metadata.add(prefix+card.getKey(), card.getValue().trim());
						if (card.getComment() == null) {
							metadata.add(prefix+card.getKey()," ");
						}
						else {
							metadata.add(prefix+card.getKey(), card.getComment().trim());
						}
					}
					
					/*
					xhtml.startElement("tr");
					String cardKey = card.getKey();
					String cardValue = card.getValue();
					String cardComment = card.getComment();
					if (cardKey == null || "".equals(cardKey)) cardKey = " ";
					if (cardValue == null || "".equals(cardValue)) cardValue = " ";
					if (cardComment == null || "".equals(cardComment)) cardComment = " ";
					
					xhtml.element("td", cardKey);
					xhtml.element("td", cardValue);
					xhtml.element("td", cardComment);
					xhtml.endElement("tr");
					*/
				}
				//xhtml.endElement("table");
				//xhtml.endElement("section");
				
				if (hdu instanceof TableHDU) {
					TableHDU table = (TableHDU) hdu;
					int nCols = table.getNCols();
					int nRows = table.getNRows();

					metadata.add(FITS_TABLE_HDU_NUMBERS, String.valueOf(i));
					
					xhtml.startElement("section","id",String.valueOf(i));
					
					AttributesImpl attributes = new AttributesImpl();
					attributes.addAttribute("", "id", "id", "CDATA", String.valueOf(i));
					attributes.addAttribute("", "columns", "columns", "CDATA", String.valueOf(nCols));
					xhtml.startElement("table",attributes);

					String extensionType = hdu.getHeader().getStringValue("XTENSION");
					
					xhtml.startElement("th", "info", "columnNames");
					for (int j=1; j<=nCols; j++) {
						String columnName = hdu.getHeader().getStringValue("TTYPE"+j);
						xhtml.element("td", columnName);
					}
					xhtml.endElement("th");
					
					xhtml.startElement("th", "info", "columnTypes");
					for (int j=1; j<=nCols; j++) {
						String columnTFORM = hdu.getHeader().getStringValue("TFORM"+j);
						String columnType = getSimpleColumnType(columnTFORM,extensionType);
						xhtml.element("td", columnType);
					}
					xhtml.endElement("th");

					/* 
					xhtml.startElement("tr");
					for (int j=0; j<nCols; j++) {
						xhtml.element("th",table.getColumnName(j));
					}
					xhtml.endElement("tr");
					*/
					
					table.getData().getData(); // Preload data from the table
					
					for (int j=0; j<nRows; j++) {

						xhtml.startElement("tr");
						Object[] row = table.getRow(j);
						
						int column = 0;
						for (Object cell: row) {							
							column++;
							String type = hdu.getHeader().getStringValue("TFORM"+column);
							//System.out.println(type);
							if (cell.getClass().isArray()) {
								if (Array.getLength(cell)>1 || Array.get(cell,0).getClass().isArray()) {
									Object obj = cell;
									String s = "[ARRAY:";
									while (obj.getClass().isArray()) {
										if (!s.endsWith(":")) s += "x";
										s += Array.getLength(obj);
										obj = Array.get(obj, 0);
									}
									s +="]";
									xhtml.startElement("td");
									xhtml.characters(s);
									xhtml.endElement("td");
								}
								else {
										Object obj = Array.get(cell,0);
										//String type = obj.getClass().getSimpleName();
										xhtml.startElement("td");
										xhtml.characters(Array.get(cell,0).toString());
										xhtml.endElement("td");
								}
							}
							else {
									//String type = cell.getClass().getSimpleName();
									xhtml.startElement("td");
									xhtml.characters(cell.toString());
									xhtml.endElement("td");
							}
						}
						
						xhtml.endElement("tr");
					}
					xhtml.endElement("table");
					xhtml.endElement("section");
				}
				
				i++;
				//xhtml.endElement("section");
			}
			
			// Determine content type
			if ((metadata.get("section0:CRPIX1") != null) && ("2".equals(metadata.get("section0:NAXIS")))) {
				metadata.add(TikaCoreProperties.TYPE, "FITS:IMAGE");
			}
			else {
				metadata.add(TikaCoreProperties.TYPE, "FITS:GENERIC");
			}
			
		}
		catch (FitsException e) {
			throw new TikaException("Error parsing FITS",e);
		}

        xhtml.endDocument();
	}

	private String getSimpleColumnType(String tform, String extension) {
		if ("TABLE".equals(extension)) {
			return FITSColumnTypes.getSimpleTypeA(tform);
		}
		else {
			return FITSColumnTypes.getSimpleTypeB(tform);
		}
	}
}
