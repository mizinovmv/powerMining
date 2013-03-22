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
		String pathJsonModel = "data";
		String pathJsonModelTest = "dataTest";

//		XmlDataModelBuilder builderXml = new XmlDataModelBuilder();
//		DataModel model = builderXml.build("coolga");
//		assertNotNull(model);
//
//		DataModelAnalyzer analyzer = new DataModelAnalyzer(model);
//		DataModel tokenModel = analyzer.build(DataModelAnalyzer.Weight.TFIDF,
//				true, 0);
//		assertNotNull(tokenModel);
//		DataModelSpliter spliter = new DataModelSpliter(tokenModel, 80);
		
		String[] AnalyzerDriverDebug = { "data" };
		String[] debug = { "--input", "dataTest", "--output", "KnnDriver",
				"-t", "data", "--overwrite", "-tc", "Analyzer/part-r-00000" };
		BufferedWriter writer = null;
		BufferedReader reader = null;

		try {
			AnalyzerDriver.run(AnalyzerDriverDebug);
			KnnDriver.NN = 99;
			while (KnnDriver.NN < 145) {
				++KnnDriver.NN;
				ToolRunner.run(new Configuration(), new KnnDriver(), debug);

				reader = new BufferedReader(new FileReader(
						"KnnDriver/part-r-00000"));
				writer = new BufferedWriter(new FileWriter("KnnDriverTest",
						true));
				String line = null;
				double[] values = new double[2];
				int count = 0;
				while ((line = reader.readLine()) != null) {
					String[] tmp = line.split("\t");
					values[count] = Double.valueOf(tmp[1]);
					++count;
				}
				writer.write(String.valueOf(KnnDriver.NN) + "\t"
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