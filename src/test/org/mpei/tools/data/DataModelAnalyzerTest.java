package org.mpei.tools.data;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.junit.Assert;
import org.junit.Test;
import org.mpei.data.analyzer.AnalyzerDriver;
import org.mpei.tools.data.DataModelAnalyzer.Weight;

public class DataModelAnalyzerTest extends Assert {

	@Test
	public void test() {
		String pathModel = "dataModel";
		String pathTokenModel = "tokenDataModel";
		String pathJsonModel = "data";
		String pathJsonModelTest = "dataTest";
		DataOutputStream out = null;
		// JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
		// DataModel model = builder
		// .build("https://classification-mizinov.rhcloud.com/api/");
		// try {
		// FileOutputStream fstream = new FileOutputStream(new File(pathModel));
		// out = new DataOutputStream(fstream);
		// // model.write(out);
		// } catch (Exception e) {
		// throw new RuntimeException(e);
		// }
		XmlDataModelBuilder builderXml = new XmlDataModelBuilder();
		DataModel model = builderXml.build("coolga");
		assertNotNull(model);
		// DataModel model = DataModel.read(pathModel);
		DataModelAnalyzer analyzer = new DataModelAnalyzer(model);
		DataModel tokenModel = analyzer.build(Weight.TF, true, 50);
//		try {
//			FileOutputStream fstream = new FileOutputStream(new File(
//					pathTokenModel));
//			out = new DataOutputStream(fstream);
//			tokenModel.write(out);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
		// DataModel tokenModel = DataModel.read(pathTokenModel);
		assertNotNull(tokenModel);
		DataModelSpliter spliter = new DataModelSpliter(tokenModel, 80);
		DataModelAnalyzer.toJSON(spliter.getTestData(), pathJsonModelTest);
		DataModelAnalyzer.toJSON(spliter.getTrainingData(), pathJsonModel);
		String[] debug = { "data" };
		try {
			AnalyzerDriver.run(debug);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		new File(pathJsonModel).delete();
		new File(pathJsonModel).delete();
		// DataModel jsonModel = new DataModel();
		// try {
		// DataModelAnalyzer.fromJSON(jsonModel, pathJsonModel);
		// } catch (Exception e) {
		// throw new RuntimeException(e);
		// }
		// assertEquals(tokenModel, jsonModel);
	}

}
