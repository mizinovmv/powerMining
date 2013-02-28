package org.mpei.tools.data;

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.junit.Before;
import org.junit.Test;
import org.mpei.tools.data.DataModelAnalyzer.Weight;

public class DataModelAnalyzerTest {
	
	String pathModel = "dataModel";
	String pathTokenModel = "tokenDataModel";
	DataModelAnalyzer analyzer;

	@Before
	public void setUp() throws Exception {
		DataModel model = DataModel.read(pathModel);
		analyzer = new DataModelAnalyzer(model);
	}

	@Test
	public void testBuild() throws Exception{
		DataModel tokenModel = analyzer.build(Weight.TFIDF, true);
		assertNotNull(tokenModel);
		FileOutputStream fstream = new FileOutputStream(new File(pathTokenModel));
		DataOutputStream out = new DataOutputStream(fstream);
		tokenModel.write(out);
	}

	@Test
	public void testToJSON() {
		DataModel tokenModel= DataModel.read(pathTokenModel);
		DataModelAnalyzer.toJSON(tokenModel, "testpath");
	}

}
