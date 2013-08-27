package edu.jhu.pha.vospace.process.tika;


import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.python.core.PyList;
import org.python.core.PyTuple;
import org.python.util.PythonInterpreter;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import edu.jhu.pha.vospace.process.tika.ATpyType.Type;


public class ATpyParser implements Parser {
	private Set<MediaType> SUPPORTED_TYPES;
	private static final MediaTypeRegistry MIME_REGISTRY = new MimeTypes().getMediaTypeRegistry();
	
	public ATpyParser() {
		SUPPORTED_TYPES = new HashSet<MediaType>();
		SUPPORTED_TYPES.add(MediaType.TEXT_PLAIN);
		SUPPORTED_TYPES.add(MediaType.application("x-votable+xml"));
	}
	
	@Override
	public Set<MediaType> getSupportedTypes(ParseContext context) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(InputStream stream, ContentHandler handler, Metadata metadata,
			ParseContext context) throws IOException, SAXException, TikaException {

		try {
			MediaType fileMediaType = MediaType.parse(metadata.get(Metadata.CONTENT_TYPE));
			String pyType;
			if(MIME_REGISTRY.isInstanceOf(fileMediaType, MediaType.TEXT_PLAIN)){
				pyType = "ascii";
			} else if(MIME_REGISTRY.isInstanceOf(fileMediaType, MediaType.application("x-votable+xml"))){
				pyType = "vo";
			} else {
				throw new TikaException("Error! Mimetype "+fileMediaType.toString()+" is not suported by AtpyParser");
			}
			
			ATpyTable table = parseATpy(metadata.get(Metadata.CONTENT_LOCATION),"127.0.0.1",8888, pyType);
			
	        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
	        
	        xhtml.startDocument();
	
	        int nCols = table.getColumns();
	        
			xhtml.startElement("section","id",String.valueOf(1));
			xhtml.startElement("table","columns",String.valueOf(nCols));
	
			xhtml.startElement("th", "info", "columnNames");
			for (int i=0; i<nCols; i++) {
	    		xhtml.element("td", table.getColumnName(i));
	    	}
			xhtml.endElement("th");
	
			xhtml.startElement("th", "info", "columnTypes");
			for (int i=0; i<nCols; i++) {
				ATpyType type = table.getColumnType(i);
				int dataType;
				switch (type.getKind()) {
					case STRING: 
						dataType = DataTypes.STRING; 
						break;
					case INT: 
						dataType = DataTypes.INT64;
						break;
					case FLOAT:
						dataType = DataTypes.DOUBLE;
						break;
					default: 
						dataType = DataTypes.UNKNOWN;
						break;
				}
	    		String columnType = DataTypes.getCharCode(dataType);
	    		xhtml.element("td", columnType);
	    	}
			xhtml.endElement("th");
			
			List<String[]> rows = table.getRows();
			
			for (int r=0; r<rows.size(); r++) {
				xhtml.startElement("tr");
				String[] row = rows.get(r);
				for (int c=0; c<nCols; c++) {
					String value = row[c];
					if ("".equals(value)) value = " ";
					xhtml.element("td", value);
				}
				xhtml.endElement("tr");
			}
				
			xhtml.endElement("table");
			xhtml.endElement("section");
			xhtml.endDocument();
			
	        metadata.add(TikaCoreProperties.TYPE, "ATpy");
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			throw new TikaException("Error! "+e.getMessage()+"\n"+sw.toString());
		}
	}

	private ATpyTable parseATpy(String sourceUrl, String pythonHost, int pythonPort, String type) throws TikaException {
		String parseFromUrl = "\"\"\""
				+ "import os, tempfile, urllib2, atpy\n"
				+ "h,fname = tempfile.mkstemp()\n"
				+ "f = os.fdopen(h,'w')\n"
				+ "try:\n"
				+ "    u = urllib2.urlopen('"+ sourceUrl + "')\n"
				+ "    f.write(u.read())\n"
				+ "    f.close()\n"
				+ "    tbl = atpy.Table(fname,type='"+type+"')\n"
				+ "finally:\n"
				//+ "    os.close(h)\n"	
				+ "    os.remove(fname)\n"
				+ "tableRows = tbl.data.tolist()\n"
				+ "columnNames = tbl.columns.keys\n"
				+ "columnTypes = [(tbl.columns[i].dtype.kind,tbl.columns[i].dtype.itemsize) for i in tbl.columns]\n"
				+ "channel.send(tableRows)\n" + "channel.send(columnNames)\n"
				+ "channel.send(columnTypes)" + "\"\"\"";
		
		ATpyTable tbl = null;
		try {
			
			PythonInterpreter interp = new PythonInterpreter();
			interp.exec("import execnet");
			interp.exec("gw = execnet.SocketGateway('"+pythonHost+"',"+pythonPort+")");
			
			try {
				interp.exec("channel = gw.remote_exec(" + parseFromUrl + ")");
	
				interp.exec("tableRows = channel.receive()");
				interp.exec("columnNames = channel.receive()");
				interp.exec("columnTypes = channel.receive()");
	
				PyList tableRows = (PyList) interp.get("tableRows");
				PyList columnNames = (PyList) interp.get("columnNames");
				PyList columnTypes = (PyList) interp.get("columnTypes");
				String[] names = (String[]) columnNames
						.toArray(new String[] {});
				PyTuple[] types = (PyTuple[]) columnTypes
						.toArray(new PyTuple[] {});
				int n = names.length;
				tbl = new ATpyTable(n);
				tbl.setAllColumnNames(names);
				ATpyType[] myTypes = new ATpyType[n];
				for (int i = 0; i < n; i++) {
					switch (types[i].get(0).toString().charAt(0)) {
					case 'i':
					case 'u':
						myTypes[i] = new ATpyType(Type.INT,((Integer) types[i].get(1)));
						break;
					case 'f':
						myTypes[i] = new ATpyType(Type.FLOAT,((Integer) types[i].get(1)));
						break;
					case 'S':
					case 'a':
					case 'U':
						myTypes[i] = new ATpyType(Type.STRING,((Integer) types[i].get(1)));
						break;
					default:
						break;
					}
				}
				tbl.setAllColumnTypes(myTypes);
				for (int i = 0; i < tableRows.size(); i++) {
					PyTuple t = (PyTuple) tableRows.get(i);
					String[] s = new String[n];
					for (int j = 0; j < n; j++) {
						s[j] = t.get(j).toString();
					}
					tbl.addRow(s);
				}
	
			} finally {
				interp.exec("gw.exit()");
			}
			
		}
		catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			throw new TikaException("Error parsing ATpy table "+sourceUrl+": "+e.getMessage()+"\n"+sw.toString());
		}
		
		return tbl;
	}
	
	public static void main(String[] args) throws Exception {
//		String sourceUrl = "http://localhost/tmp/ExportTable_dmedv.csv";
//		Parser parser = new ATpyParser();
//
//		Metadata metadata = new Metadata();
//		metadata.set(Metadata.CONTENT_LOCATION,sourceUrl);
//		
//		AsciiTableContentHandler handler = new AsciiTableContentHandler();
//		
//		parser.parse((new URL(sourceUrl)).openStream(), handler, metadata, new ParseContext());
//		
//		metadata.set(TikaCoreProperties.SOURCE,sourceUrl);
//		Database db = new SQLShare();
//		db.setup();
//		db.update(metadata, (ArrayList)handler.getTables());
//		db.close();
		ATpyParser parser = new ATpyParser();
		
//		System.out.println(parser.parseATpy("http://dimmnb/vospace-2.0/data/4201a3bd-495b-4a9c-80eb-d4d16974aa23",
//				"127.0.0.1",8888, "ascii"));

		System.out.println(parser.parseATpy("http://dimmnb/vospace-2.0/data/8950c206-dda2-4d80-ba33-384cb1346743",
				"127.0.0.1",8888, "vo"));
		
	}
}
