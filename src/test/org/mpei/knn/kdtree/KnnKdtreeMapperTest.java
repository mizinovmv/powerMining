package org.mpei.knn.kdtree;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.mpei.data.analyzer.AnalyzerDriver;
import org.mpei.knn.KnnDriver;

public class KnnKdtreeMapperTest {
	@Test
	public void testRun() {
		String trainData = "tfc";
		String testData = "tfcTest";

		String[] AnalyzerDriverDebug = { trainData };
		BufferedWriter writer = null;
		BufferedReader reader = null;

		try {
			AnalyzerDriver.run(AnalyzerDriverDebug);
			int i = -1;
			while (i < 120) {
				i += 2;
				String[] debug = { "--input", testData, "--output",
						"KnnKdtreeDriver", "-t", trainData, "--overwrite", "-tc",
						"Analyzer/part-r-00000", "-nn", String.valueOf(i),
						"-ts", "150" };
				ToolRunner.run(new Configuration(), new KnnKdtreeDriver(),
						debug);

				reader = new BufferedReader(new FileReader(
						"KnnKdtreeDriver/part-r-00000"));
				writer = new BufferedWriter(new FileWriter(
						"KnnKdtreeDriverTest", true));
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
