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
package edu.jhu.pha.vospace.rest;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.generationjava.io.xml.SimpleXmlWriter;

import edu.jhu.pha.vospace.DbPoolServlet;
import edu.jhu.pha.vospace.DbPoolServlet.SqlWorker;
import edu.jhu.pha.vospace.SettingsServlet;
import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;

/**
 * Provides the REST service for / path: the functions for manipulating the observatory capabilities and metadata
 * @author Dmitry Mishin
 */
@Path("/")
public class MetadataController {
	private static final Logger logger = Logger.getLogger(MetadataController.class);
	private static Configuration conf = SettingsServlet.getConfig();;
	
	/**
	 * Returns the observatory supported protocols
	 * @return The supported protocols in VOSpace xml format
	 */
	@GET @Path("/protocols")
	@Produces(MediaType.APPLICATION_XML)
	public String getProtocols() {
		StringWriter writ = new StringWriter();
		SimpleXmlWriter xw = new SimpleXmlWriter(writ);
		try {
			xw.writeEntity("protocols");
			
			xw.writeEntity("accepts");
			for(String protocol: (List<String>)conf.getList("transfers.protocols.accepts")){
				xw.writeEntity("protocol").writeAttribute("uri", protocol).endEntity();
			}
			xw.endEntity();
			
			xw.writeEntity("provides");
			for(String protocol: (List<String>)conf.getList("transfers.protocols.provides")){
				xw.writeEntity("protocol").writeAttribute("uri", protocol).endEntity();
			}
			xw.endEntity();
			
			xw.endEntity();
			xw.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
		return writ.toString();
	}

	/**
	 * Returns the observatory supported views
	 * @return The supported views in VOSpace xml format
	 */
	@GET @Path("/views")
	@Produces(MediaType.APPLICATION_XML)
	public String getViews() {
		StringWriter writ = new StringWriter();
		SimpleXmlWriter xw = new SimpleXmlWriter(writ);
		try {
			//org.iso_relax.verifier.Verifier verifier = org.iso_relax.verifier.VerifierFactory.newInstance("http://www.w3.org/2001/XMLSchema").newVerifier(new URL("http://dimm.wdcb.ru/pub/tmp/scheme.xsd").openStream());
			//JarvWriter xw = new JarvWriter(new SimpleXmlWriter(writ), verifier.getVerifierHandler());

			xw.writeEntity("views");
			
			xw.writeEntity("accepts");
			for(String view: (List<String>)conf.getList("core.views.accepts")){
				xw.writeEntity("view").writeAttribute("uri", view).endEntity();
			}
			xw.endEntity();
			
			xw.writeEntity("provides");
			for(String view: (List<String>)conf.getList("core.views.provides")){
				xw.writeEntity("view").writeAttribute("uri", view).endEntity();
			}
			xw.endEntity();
			
			xw.endEntity();
			xw.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
		return writ.toString();
	}
	
	/**
	 * Returns the observatory supported properties
	 * @return The supported properties in VOSpace xml format
	 */
	@GET @Path("/properties")
	@Produces(MediaType.APPLICATION_XML)
	public String getProperties() {
		StringWriter writ = new StringWriter();
		final SimpleXmlWriter xw = new SimpleXmlWriter(writ);
		try {
			xw.writeEntity("properties");
			
			xw.writeEntity("accepts");
			for(String prop: (List<String>)conf.getList("core.properties.accepts")){
				xw.writeEntity("property").writeAttribute("uri", prop).endEntity();
			}
			xw.endEntity();
			
			xw.writeEntity("provides");
			for(String prop: (List<String>)conf.getList("core.properties.provides")){
				xw.writeEntity("property").writeAttribute("uri", prop).endEntity();
			}
			xw.endEntity();

			xw.writeEntity("contains");
			
	        DbPoolServlet.goSql("Retrieving all properties",
	                "SELECT property_uri from properties",
	                new SqlWorker<Boolean>() {
	                    @Override
	                    public Boolean go(Connection conn, PreparedStatement stmt) throws SQLException {
	                        ResultSet rs = stmt.executeQuery();
	                        while (rs.next()) {
	                        	try {
	                        		xw.writeEntity("property").writeAttribute("uri", rs.getString(1)).endEntity();
	                        	} catch(IOException ex) {logger.error(ex.getMessage());};
	                        }
	                    	return true;
	                    }
	                }
	        );
			
			xw.endEntity();

			xw.endEntity();
			xw.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
		return writ.toString();
	}
}
