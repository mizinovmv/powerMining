package org.mpei.tools.data;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.junit.Assert;
import org.junit.Test;
import org.mpei.data.analyzer.AnalyzerDriver;
import org.mpei.tools.data.DataModelAnalyzer.Weight;

public class DataModelAnalyzerTest extends Assert {
	static final String pathModel = "dataModel";
	static final String pathTokenModel = "tokenDataModel";
	static final String pathJsonModel = "tfCoolga";
	static final String pathJsonModelTest = "tfCoolgaTest";

	static final String pathXmlBorodkin = "resources/borodkin/";
	static final String pathXmlCoolga = "resources/coolga/";
	static final String pathXmlMizinov = "resources/mizinov/";
	@Test
	public void testTf() {
	
		XmlDataModelBuilder builderXml = new XmlDataModelBuilder();
		DataModel model = builderXml.build(pathXmlCoolga, 0);
		assertNotNull(model);

		DataModelAnalyzer analyzer = new DataModelAnalyzer(model);
		DataModel tokenModel = analyzer.build(DataModelAnalyzer.Weight.TF,
				true, 0);
		assertNotNull(tokenModel);
		DataModelSpliter spliter = new DataModelSpliter(tokenModel, 80);
		assertNotNull(spliter);
		assertNotNull(spliter.getTestData());
		assertNotNull(spliter.getTrainingData());
		DataModelAnalyzer.toJSON(spliter.getTestData(), pathJsonModelTest);
		DataModelAnalyzer.toJSON(spliter.getTrainingData(), pathJsonModel);
	}
}
