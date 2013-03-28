package org.mpei.kmeans;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.mpei.data.analyzer.AnalyzerDriver;
import org.mpei.kmeans.train.KMeansDriverTrain;
import org.mpei.tools.data.DataModel;
import org.mpei.tools.data.DataModelAnalyzer;
import org.mpei.tools.data.DataModelSpliter;
import org.mpei.tools.data.XmlDataModelBuilder;
import org.mpei.tools.data.DataModelAnalyzer.Weight;

public class KMeansDriverTest {

	@Test
	public void testRun() {
		String data = "tfCoolga";
		String dataTest = "tfCoolgaTest";

		String[] AnalyzerDriverDebug = { data };
		String[] KMeansDriverTrainDebug = { "--input", data, "--output",
				"KMeansDriverTrain", "--overwrite" };

		BufferedWriter writer = null;
		BufferedReader reader = null;
		try {
			AnalyzerDriver.run(AnalyzerDriverDebug);
			int i = -50;
			while (i < 10000) {
				i += 50;
				String[] debug = { "--input", dataTest, "--output",
						"KMeansDriver", "-means", "KMeansDriverTrain",
						"--overwrite", "-tc", "Analyzer/part-r-00000", "-ts",
						String.valueOf(i) };
				ToolRunner.run(new Configuration(), new KMeansDriverTrain(),
						KMeansDriverTrainDebug);
				ToolRunner.run(new Configuration(), new KMeansDriver(), debug);

				reader = new BufferedReader(new FileReader(
						"KMeansDriver/part-r-00000"));
				writer = new BufferedWriter(new FileWriter("KMeansDriverTest",
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
