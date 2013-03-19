package org.mpei.knn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.mpei.data.document.DocumentInputFormat;
import org.mpei.json.JsonInputFormat;
import org.mpei.knn.step2.Mapper2;
import org.mpei.knn.step2.Reducer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnnDriver extends AbstractJob {

	private static final Logger LOG = LoggerFactory.getLogger(KnnDriver.class);

	public static final String NEIGHBORS = "mapred.conf.neighbors";
	public static final String TOKEN_CASHE = "mapred.conf.tokencashe";
	public static final String TRAIN = "mapred.conf.train";
	public static final String METRIC = "mapred.conf.metric";
	public static int NN = 1;

	public static void main(String[] args) throws Exception {
		String[] debug = { "--input", "dataTest", "--output", "KnnDriver",
				"-t", "data", "--overwrite", "-tc", "Analyzer/part-r-00000" };
		while (NN < 145) {
			ToolRunner.run(new Configuration(), new KnnDriver(), debug);
			++NN;
		}
	}

	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();
		addOption(DefaultOptionCreator.overwriteOption().create());
		addOption(KnnDriver.TOKEN_CASHE, "tc", "Path with token's vocabulary.",
				"./");
		addOption(KnnDriver.NEIGHBORS, "nn",
				"number of search nearest neghbors.", "1");
		addOption(KnnDriver.TRAIN, "t", "train data.", "./");
		addOption(KnnDriver.METRIC, "m", "Metric type.", "2");
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
				DocumentInputFormat.class, KnnMapper.class, Text.class,
				IntWritable.class, IntSumReducer.class, Text.class,
				IntWritable.class, TextOutputFormat.class, getConf());
		job.setJobName("KnnDriver");

		job.getConfiguration().set(TOKEN_CASHE, getOption(TOKEN_CASHE));
		job.getConfiguration().set(TRAIN, getOption(TRAIN));
		job.getConfiguration().set(METRIC, getOption(METRIC));
		job.getConfiguration().set(NEIGHBORS, String.valueOf(NN));

		cacheTrainData(job.getConfiguration());
		URI uriTokens = new Path(job.getConfiguration().get(TOKEN_CASHE))
				.toUri();
		DistributedCache.addCacheFile(uriTokens, job.getConfiguration());

		boolean succeeded = job.waitForCompletion(true);

		logReadable(job, output);
		return succeeded ? 0 : -1;
	}

	void cacheTrainData(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(conf.get(TRAIN));
		FileStatus[] status = fs.listStatus(hdfsPath);
		for (FileStatus st : status) {
			try {
				DistributedCache.addCacheFile(new URI(st.getPath().toString()),
						conf);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void logReadable(Job job, Path output) throws IOException {
		FileSystem fs = null;
		BufferedReader buffReader = null;
		BufferedWriter buffWriter = null;
		FileWriter fstream = new FileWriter("error.txt", true);
		try {
			fs = FileSystem.get(job.getConfiguration());
			FSDataInputStream fsIn = fs
					.open(new Path(output + "/part-r-00000"));
			buffReader = new BufferedReader(new InputStreamReader(fsIn));
			buffWriter = new BufferedWriter(fstream);
			String line = null;
			double[] values = new double[2];
			int count = 0;
			while ((line = buffReader.readLine()) != null) {
				String[] tmp = line.split("\t");
				values[count] = Double.valueOf(tmp[1]);
				++count;
			}
			int NN = Integer.valueOf(job.getConfiguration().get(NEIGHBORS));
			buffWriter.write(String.valueOf(NN) + "\t"
					+ String.valueOf(values[0] / (values[0] + values[1]))
					+ "\n");
		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		} finally {
			buffReader.close();
			buffWriter.close();
			fs.close();
		}
	}
}
