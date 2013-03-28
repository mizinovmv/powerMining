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
	public static final String TOKEN_SIZE = "mapred.conf.tokensize";

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
		addOption(KMeansDriver.TOKEN_SIZE, "ts", "Token size.", "0");
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
		job.getConfiguration().set(TOKEN_SIZE, getOption(TOKEN_SIZE));
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
}
