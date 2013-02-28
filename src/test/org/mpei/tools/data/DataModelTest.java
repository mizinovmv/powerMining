package org.mpei.tools.data;

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.junit.Before;
import org.junit.Test;

public class DataModelTest {
	String path = "dataModel";
	DataModel model;
	DataOutputStream out;
	@Before
	public void setUp() throws Exception {
		
		
	}
	@Test
	public void testWrite() throws Exception{
		JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
		model = builder
				.build("https://classification-mizinov.rhcloud.com/api/");
		FileOutputStream fstream = new FileOutputStream(new File(path));
		out = new DataOutputStream(fstream);
		model.write(out);
		out.close();
	}

	@Test
	public void testRead() {
		JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
		model = builder
				.build("https://classification-mizinov.rhcloud.com/api/");
		DataModel testModel = DataModel.read(path);
		assertNotNull(testModel);
		assertNotNull(model);
		assertEquals(testModel, model);
	}

	@Test
	public void testAddDocumentsStringDocumentArray() {
		fail("Not yet implemented");
	}

	@Test
	public void testAddDocumentsTextDocumentArray() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetLabels() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetDocuments() {
		fail("Not yet implemented");
	}


	@Test
	public void testReadFields() {
		fail("Not yet implemented");
	}

	@Test
	public void testToString() {
		fail("Not yet implemented");
	}

}
