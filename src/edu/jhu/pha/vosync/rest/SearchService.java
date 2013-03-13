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
package edu.jhu.pha.vosync.rest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.TokenBuffer;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.api.AccountInfo;
import edu.jhu.pha.vospace.jobs.JobsProcessor;
import edu.jhu.pha.vospace.meta.MetaStoreDistributed;
import edu.jhu.pha.vospace.meta.MetaStore;
import edu.jhu.pha.vospace.meta.MetaStoreFactory;
import edu.jhu.pha.vospace.meta.RegionsInfo;
import edu.jhu.pha.vospace.node.ContainerNode;
import edu.jhu.pha.vospace.node.DataNode;
import edu.jhu.pha.vospace.node.Node;
import edu.jhu.pha.vospace.node.NodeFactory;
import edu.jhu.pha.vospace.node.NodePath;
import edu.jhu.pha.vospace.node.NodeType;
import edu.jhu.pha.vospace.node.VospaceId;
import edu.jhu.pha.vospace.node.Node.Detail;
import edu.jhu.pha.vospace.oauth.DropboxAccessLevel;
import edu.jhu.pha.vospace.oauth.UserHelper;
import edu.jhu.pha.vospace.rest.JobDescription;
import edu.jhu.pha.vospace.rest.JobDescription.DIRECTION;
import edu.jhu.pha.vosync.exception.BadRequestException;
import edu.jhu.pha.vosync.exception.ForbiddenException;
import edu.jhu.pha.vosync.exception.InternalServerErrorException;
import edu.jhu.pha.vosync.exception.NotAcceptableException;
import edu.jhu.pha.vosync.exception.NotFoundException;
import org.apache.lucene.document.Document;

/**
 * @author Dmitry Mishin
 */
@Path("/search")
public class SearchService {
	
	private static final Logger logger = Logger.getLogger(SearchService.class);
	private @Context ServletContext context;
	private @Context HttpServletRequest request;
	private static final Configuration conf = SettingsServlet.getConfig();

	private static final JsonFactory f = new JsonFactory();
	
    private static Analyzer analyzer = new EnglishAnalyzer(Version.LUCENE_41);

	
	@Path("")
	@GET
	public Response search(@QueryParam("query") String queryStr) {
		final String username = (String)request.getAttribute("username");
		
		try {
			Directory directory = FSDirectory.open(new File(conf.getString("lucene.index")));
			
		    DirectoryReader ireader = DirectoryReader.open(directory);
		    IndexSearcher isearcher = new IndexSearcher(ireader);
		    
		    QueryParser parser = new QueryParser(Version.LUCENE_41, "content", analyzer);
		    String queryFullStr = "owner:\""+username+"\" AND "+queryStr;
		    Query query = parser.parse(queryFullStr);
		    ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
		    
		    StringBuffer buf = new StringBuffer();
		    
		    for (int i = 0; i < hits.length; i++) {
		      Document hitDoc = isearcher.doc(hits[i].doc);
		      buf.append(hitDoc.get("contents"));
		    }
		    ireader.close();
		    directory.close();

		    return Response.ok(buf.toString()).build();
		} catch(Exception ex) {
			ex.printStackTrace();
			return Response.ok().build();
		}
	}
	
	public static final void main(String[] s) {
		try {
			Directory directory = FSDirectory.open(new File("/Users/dimm/tmp/lucene"));
			
			ResponseBuilder resp = Response.ok();
			
			// Now search the index:
		    DirectoryReader ireader = DirectoryReader.open(directory);
		    IndexSearcher isearcher = new IndexSearcher(ireader);
		    // Parse a simple query that searches for "text":
		    QueryParser parser = new QueryParser(Version.LUCENE_41, "content", analyzer);

		    String queryS = "owner:\"https://vaossotest.ncsa.illinois.edu/openid/id/dimm\" AND destructor";

		    Query query = parser.parse(queryS);
		    ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
		    // Iterate through the results:
		    
		    System.out.println(hits.length);
		    
		    for (int i = 0; i < hits.length; i++) {
		      Document hitDoc = isearcher.doc(hits[i].doc);
		      System.out.println(hitDoc.get("content"));
		    }
		    ireader.close();
		    directory.close();
		    
		} catch(Exception ex) {
			ex.printStackTrace();
		}

	}
}
