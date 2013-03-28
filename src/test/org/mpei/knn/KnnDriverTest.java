package org.mpei.knn;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.mpei.data.analyzer.AnalyzerDriver;
import org.mpei.tools.data.DataModel;
import org.mpei.tools.data.DataModelAnalyzer;
import org.mpei.tools.data.DataModelSpliter;
import org.mpei.tools.data.XmlDataModelBuilder;

public class KnnDriverTest {

	@Test
	public void testRun() {
		String pathModel = "dataModel";
		String pathTokenModel = "tokenDataModel";
		String pathJsonModel = "data2";
		String pathJsonModelTest = "dataTest2";

		String pathXmlBorodkin = "resources/borodkin/";
		String pathXmlCoolga = "resources/coolga/";
		String pathXmlMizinov = "resources/mizinov/";

//		XmlDataModelBuilder builderXml = new XmlDataModelBuilder();
//		DataModel model = builderXml.build(pathXmlBorodkin, 0);
//		assertNotNull(model);
//
//		DataModelAnalyzer analyzer = new DataModelAnalyzer(model);
//		DataModel tokenModel = analyzer.build(DataModelAnalyzer.Weight.TF,
//				true, 0);
//		assertNotNull(tokenModel);
//		DataModelSpliter spliter = new DataModelSpliter(tokenModel, 80);
//		assertNotNull(spliter);
//		assertNotNull(spliter.getTestData());
//		assertNotNull(spliter.getTrainingData());
//		DataModelAnalyzer.toJSON(spliter.getTestData(), pathJsonModelTest);
//		DataModelAnalyzer.toJSON(spliter.getTrainingData(), pathJsonModel);

		String[] AnalyzerDriverDebug = { pathJsonModel};

		BufferedWriter writer = null;
		BufferedReader reader = null;

		try {
			AnalyzerDriver.run(AnalyzerDriverDebug);
			int i = 85;
			while (i < 120) {
				i+=2;
				String[] debug = { "--input", pathJsonModelTest, "--output",
						"KnnDriver2", "-t", pathJsonModel, "--overwrite", "-tc",
						"Analyzer/part-r-00000", "-nn",String.valueOf(i),"-ts",
						"150" };
				ToolRunner.run(new Configuration(), new KnnDriver(), debug);

				reader = new BufferedReader(new FileReader(
						"KnnDriver2/part-r-00000"));
				writer = new BufferedWriter(new FileWriter("KnnDriverTest2",
						true));
				String line = null;
				double[] values = new double[2];
				int count = 0;
				while ((line = reader.readLine()) != null) {
					String[] tmp = line.split("\t");
					values[count] = Double.valueOf(tmp[1]);
					++count;
				}
				writer.write(String.valueOf(i) + "\t"
						+ String.valueOf(values[0] / (values[0] + values[1]))
						+ "\n");
				writer.flush();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
				if (reader != null) {
					reader.close();
				}
			} catch (Exception e2) {
				throw new RuntimeException(e2);
			}

		}
	}

}
