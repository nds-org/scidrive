package edu.jhu.pha.vospace.rest;

import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.path.xml.XmlPath.from;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.http.client.ClientProtocolException;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NodesControllerTest {

	static final String filesDir = "/Users/dmitry/Documents/workspace/scidrive/test-xml/";
	static final String transfersUrl = "http://dimm.pha.jhu.edu:8080/vospace-2.0/transfers";
	
	@BeforeClass
    public static void setUp() {
		RestAssured.baseURI = "http://dimm.pha.jhu.edu";
		RestAssured.port = 8080;
		RestAssured.basePath = "/vospace-2.0";
		RestAssured.authentication = KeystoneAuth.getInstance();
    }
	
	@Test
	public void _010testProtocols() {
		
		expect().
			body("protocols.accepts.protocol.@uri",	hasItems(
					"ivo://ivoa.net/vospace/core#httpget",
					"ivo://ivoa.net/vospace/core#httpput"
					)).
			body("protocols.provides.protocol.@uri",	hasItems(
					"ivo://ivoa.net/vospace/core#httpget",
					"ivo://ivoa.net/vospace/core#httpput"
					)).
		when().
			get("/protocols");
	}

	@Test
	@Ignore
	public void _020testContRegions() {
		expect().
			statusCode(200).
			log().all().
		given().
			get("/1/regions/sync1");
	}


	@Test
	public void _030deleteTestContInit() {
		expect().
			statusCode(204).
		given().
			delete("/nodes/test_cont1");
	}

	@Test
	public void _050testPutNewContNode() {
		expect().
			statusCode(201).
		given().
			content(getFileContents("newContainerNode.xml")).
			put("/nodes/test_cont1");
	}

	@Test
	public void _060testPutNewDataNode() {
		expect().
			statusCode(201).
		given().
			content(getFileContents("newDataNode1.xml")).
			put("/nodes/test_cont1/data1.bin");
	}

	@Test
	public void _070testPutNewStructuredNode() {
		expect().
			statusCode(201).
		given().
			content(getFileContents("newDataNode2.xml")).
			put("/nodes/test_cont1/data2.bin");
	}

	@Test
	public void _080testPutNewUnstructuredNode() {
		expect().
			statusCode(201).
		given().
			content(getFileContents("newDataNode3.xml")).
			put("/nodes/test_cont1/data3.bin");
	}

	@Test
	public void _090testSetDataNode() {
		expect().
			statusCode(200).
			body("node.properties.property.findAll{it.@uri == 'ivo://ivoa.net/vospace/core#my_date'}", equalTo("2010-03-12")).
		given().
			content(getFileContents("setDataNode.xml")).
			post("/nodes/test_cont1/data1.bin");
	}

	@Test
	public void _120testPullToVoSpace() throws ClientProtocolException, IOException, InterruptedException {
		String jobXml = expect().
				statusCode(200).
			given().
				content(getFileContents("pullToVoSpace.xml")).
				post("/transfers").body().asString();
		/*HttpResponse resp = postJob("pullToVoSpace.xml");
		String jobXml = streamToString(resp.getEntity().getContent());*/
		ArrayList jobDetailsUrl = from(jobXml).get("job.results.result.findAll{it.@id == 'transferDetails'}.@href");
		String jobId = from(jobXml).get("job.jobId");

		assertThat("ERROR",not(equalTo(from(jobXml).get("job.phase"))));
		
		int tries = 50;
		String status = "";
		while(tries > 0 && !status.equals("COMPLETED")){
			Response resp2 = expect().
				statusCode(200).
				body(not(equalTo("ERROR"))).
			given().
				get(transfersUrl+"/"+jobId+"/phase");
			status = resp2.body().asString();
			tries--;
			synchronized(this) {
				this.wait(100);
			}
		}
		assertThat(tries, greaterThan(0));
	}

	@Test
	public void _130testCopyNode() throws ClientProtocolException, IOException, InterruptedException {
		String jobXml = expect().
			statusCode(200).
		given().
			content(getFileContents("copy.xml")).
			post("/transfers").body().asString();
		
		/*HttpResponse resp = postJob("copy.xml"); // if only urlencoded form
		String jobXml = streamToString(resp.getEntity().getContent());*/
		
		ArrayList jobDetailsUrl = from(jobXml).get("job.results.result.findAll{it.@id == 'transferDetails'}.@href");
		String jobId = from(jobXml).get("job.jobId");
		
		assertThat("ERROR",not(equalTo(from(jobXml).get("job.phase"))));

		int tries = 50;
		String status = "";
		while(tries > 0 && !status.equals("COMPLETED")){
			Response resp2 = expect().
				statusCode(200).
				body(not(equalTo("ERROR"))).
			given().
				get(transfersUrl+"/"+jobId+"/phase");
			status = resp2.body().asString();
			tries--;
			synchronized(this) {
				this.wait(100);
			}
		}
		assertThat(tries, greaterThan(0));
	}

	@Test
	public void _140testCopyNode2() throws ClientProtocolException, IOException, InterruptedException {
		String jobXml = expect().
			statusCode(200).
		given().
			content(getFileContents("copy2.xml")).
			post("/transfers").body().asString();
		
		ArrayList jobDetailsUrl = from(jobXml).get("job.results.result.findAll{it.@id == 'transferDetails'}.@href");
		String jobId = from(jobXml).get("job.jobId");
		
		assertThat("ERROR",not(equalTo(from(jobXml).get("job.phase"))));
		
		int tries = 50;
		String status = "";
		while(tries > 0 && !status.equals("COMPLETED")){
			Response resp2 = expect().
				statusCode(200).
				body(not(equalTo("ERROR"))).
			given().
				get(transfersUrl+"/"+jobId+"/phase");
			status = resp2.body().asString();
			tries--;
			synchronized(this) {
				this.wait(100);
			}
		}
		assertThat(tries, greaterThan(0));
	}

	@Test
	public void _150testContent() throws ClientProtocolException, IOException, InterruptedException {
		expect().
			statusCode(200).
			//body(hasXPath("/node/nodes/node[@uri='vos://dimm.pha.jhu.edu!vospace/test_cont1/pulleddata1.txt']")).
			//body(hasXPath("/node/nodes/node[@uri='vos://dimm.pha.jhu.edu!vospace/test_cont1/pulleddata2.txt']")).
			//body(hasXPath("/node/nodes/node[@uri='vos://zinc27.pha.jhu.edu!vospace/test_cont1/data4.bin']")).
			//body(hasXPath("/node/nodes/node[@uri='vos://dimm.pha.jhu.edu!vospace/test_cont1/data2.bin']")).
			body(hasXPath("/node/nodes/node[@uri='vos://dimm.pha.jhu.edu!vospace/test_cont1/data1.bin']")).
		given().
			queryParam("detail", "max").
			get("/nodes/test_cont1");

//		String pulledData2Node = expect().
//			statusCode(200).
//			body("node.properties.property.findAll{it.@uri == 'ivo://ivoa.net/vospace/core#length'}.text()", equalTo("1745106")).
//		given().
//			queryParam("detail", "max").
//			get("/nodes/test_cont1/pulleddata2.txt").body().asString();
	}

	@Test
	public void _160deleteTestCont() {
		expect().
			statusCode(204).
		given().
			delete("/nodes/test_cont1");
	}

	@Test
	public void _170deleteTestContNotFound() {
		expect().
			statusCode(404).
			body(equalTo("NodeNotFound")).
		given().
			delete("/nodes/test_cont1_not_found");
	}

	private String getFileContents(String fileName) {
		StringBuffer buf = new StringBuffer();
		
		File readFile = new File(filesDir+fileName);
		try {
			FileReader reader = new FileReader(readFile);
			char[] cbuf = new char[1024];
			int read = reader.read(cbuf);
			while(read >= 0){
				buf.append(cbuf,0,read);
				read = reader.read(cbuf);
			}
		} catch (FileNotFoundException e) {
			fail(e.getMessage());
		} catch (IOException e) {
			fail(e.getMessage());
		}
		return buf.toString();
	}
}
