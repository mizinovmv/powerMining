package org.mpei.tools.data;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.junit.Assert;
import org.junit.Test;
import org.mpei.tools.data.DataModelAnalyzer.Weight;

public class DataModelAnalyzerTest extends Assert {

	@Test
	public void test() {
		String pathModel = "dataModel";
		String pathTokenModel = "tokenDataModel";
		String pathJsonModel = "resources";
//		JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
//		DataModel model = builder
//				.build("https://classification-mizinov.rhcloud.com/api/");
//		DataOutputStream out = null;
//		try {
//			FileOutputStream fstream = new FileOutputStream(new File(pathModel));
//			out = new DataOutputStream(fstream);
//			// model.write(out);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//		// DataModel model = DataModel.read(pathModel);
//		DataModelAnalyzer analyzer = new DataModelAnalyzer(model);
//		DataModel tokenModel = analyzer.build(Weight.TFIDF, true, out);
//		try {
//			FileOutputStream fstream = new FileOutputStream(new File(
//					pathTokenModel));
//			out = new DataOutputStream(fstream);
//			tokenModel.write(out);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
		 DataModel tokenModel = DataModel.read(pathTokenModel);
		assertNotNull(tokenModel);
		DataModelAnalyzer.toJSON(tokenModel, pathJsonModel);
		DataModel jsonModel = new DataModel();
		try {
			DataModelAnalyzer.fromJSON(jsonModel, pathJsonModel);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		assertEquals(tokenModel, jsonModel);
	}

}
