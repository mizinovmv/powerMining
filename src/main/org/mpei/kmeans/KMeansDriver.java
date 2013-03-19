package org.mpei.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.mpei.data.document.Document;
import org.mpei.data.document.DocumentInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansDriver extends AbstractJob {
	static final Logger LOG = LoggerFactory.getLogger(KMeansDriver.class);
	public static final String MEANS_PATH = "mapred.conf.means";
	public static final String TOKEN_CASHE = "mapred.conf.tokencashe";
	public static final String METRIC = "mapred.conf.metric";
	
	static int loop = 0;

	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		addInputOption();
		addOutputOption();
		addOption(DefaultOptionCreator.overwriteOption().create());
		addOption(KMeansDriver.MEANS_PATH, "means", "Path with means",
				"KMeansDriverTrain");
		addOption(KMeansDriver.TOKEN_CASHE, "tc",
				"Path with token's vocabulary.", "knnAnalyzer/part-r-00000");
		addOption(KMeansDriver.METRIC, "m", "Metric type.", "2");
		Map<String, List<String>> parsedArgs = parseArguments(args);
		if (parsedArgs == null) {
			return -1;
		}
		Path input = getInputPath();
		Path output = getOutputPath();
		if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
			HadoopUtil.delete(getConf(), output);
		}
		Job job = HadoopUtil.prepareJob(input, output,
				DocumentInputFormat.class, KMeansMapper.class, Text.class,
				IntWritable.class, IntSumReducer.class, Text.class,
				Document.class, TextOutputFormat.class, getConf());
		job.setJobName("KMeansDriver");
		job.getConfiguration().set(MEANS_PATH, getOption(MEANS_PATH));
		job.getConfiguration().set(TOKEN_CASHE, getOption(TOKEN_CASHE));
		job.getConfiguration().set(METRIC, getOption(METRIC));
		cacheCntroids(job.getConfiguration());

		URI uriTokens = new Path(job.getConfiguration().get(TOKEN_CASHE))
				.toUri();
		DistributedCache.addCacheFile(uriTokens, job.getConfiguration());
		Date startTime = new Date();
		LOG.info("Job started: " + startTime);

		int res = job.waitForCompletion(true) ? 0 : 1;
		Date end_time = new Date();
		LOG.info("Job ended: " + end_time);
		LOG.info("The job took " + (end_time.getTime() - startTime.getTime())
				/ 1000 + " seconds.");
		return res;
	}

	void cacheCntroids(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(conf.get(MEANS_PATH));
		FileStatus[] status = fs.listStatus(hdfsPath);
		for (FileStatus st : status) {
			try {
				/*
				 * not status files (_SUCCESS)
				 */
				if (!st.getPath().getName().matches("^_.*")) {
					DistributedCache.addCacheFile(new URI(st.getPath()
							.toString()), conf);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

		}
		// upload the file to hdfs. Overwrite any existing copy.
		// fs.copyFromLocalFile(false, true, new Path(LOCAL_STOPWORD_LIST),
		// hdfsPath);

	}

	public static void main(String[] args) throws IOException {
		BufferedWriter writer = null;
		BufferedReader reader = null;
		String[] debug = { "--input", "dataTest", "--output", "KMeansDriver",
				"-means", "KMeansDriverTrain", "--overwrite", "-tc",
				"Analyzer/part-r-00000" };
		KMeansDriver.loop = 10000;
		while (loop < 20000) {
			KMeansDriver.loop = KMeansDriver.loop + 100;
			try {
				ToolRunner.run(new Configuration(), new KMeansDriver(), debug);
				reader = new BufferedReader(new FileReader(
						"KMeansDriver/part-r-00000"));
				writer = new BufferedWriter(new FileWriter("result", true));
				String line = null;
				writer.write(KMeansDriver.loop);
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
					writer.write(line);
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} finally {
				writer.close();
				reader.close();
			}
		}
		
		
//		BufferedReader reader = new BufferedReader(new FileReader("result"));
//		BufferedWriter writer = new BufferedWriter(new FileWriter("result2", true));
//		String line = null;
//		while ((line = reader.readLine()) != null) {
//			String loop = line;
//			double knnFalse = Double.valueOf(reader.readLine().split("\t")[1]);
//			double knnTrue = Double.valueOf(reader.readLine().split("\t")[1]);
//			writer.write(loop + " " + (knnFalse/(knnFalse + knnTrue)) + "\n");
//		}
//		writer.close();
//		reader.close();
	}
}
