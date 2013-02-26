package org.mpei.tools.data;

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Test;

public class JsonUrlDataModelBuilderTest {

	@Test
	public void testBuild() throws IOException{
		JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
		DataModel model1 = builder.build("https://classification-mizinov.rhcloud.com/api/");
		String path = "dataModel";
		FileOutputStream fstream = new FileOutputStream(new File(
				path));
		DataOutputStream out = new DataOutputStream(fstream);
		model1.write(out);
		DataModel model2 = builder.read(path);
		assertArrayEquals(model1.getLabels(), model2.getLabels());
//		assertEquals(model1, model2);
	}

}
