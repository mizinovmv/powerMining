package org.mpei.knn.gridFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.ResultAnalyzer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.mpei.data.document.DocumentFabric;
import org.mpei.data.document.DocumentInputFormat;
import org.mpei.json.JsonInputFormat;
import org.mpei.knn.KnnDriver;
import org.mpei.knn.kdtree.KnnKdtreeMapper;
import org.mpei.knn.kdtree.tools.KDTree;
import org.mpei.knn.kdtree.tools.KnnKdtreeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnnGridFilesDriver extends AbstractJob {

	private static final Logger LOG = LoggerFactory
			.getLogger(KnnGridFilesDriver.class);

	public static final String DELIMITER = "mapred.conf.delimiter";
	public static final String NEIGHBORS = "mapred.conf.neighbors";
	public static final String TOKEN_CASHE = "mapred.conf.tokencashe";
	public static final String TRAIN = "mapred.conf.train";
	public static final String METRIC = "mapred.conf.metric";
	public static final String TOKEN_SIZE = "mapred.conf.tokensize";

	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();
		addOption(DefaultOptionCreator.overwriteOption().create());
		addOption(TOKEN_CASHE, "tc", "Path with token's vocabulary.", null);
		addOption(TRAIN, "t", "Path with train data.", null);
		addOption(NEIGHBORS, "nn", "Number of nearest neighbors.", "1");
		addOption(DELIMITER, "dlm", "Delimiter for round.", "1");
		addOption(METRIC, "m", "Metric type.", "2");
		addOption(TOKEN_SIZE, "ts", "Token size.", "0");
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
				DocumentInputFormat.class, KnnGridFilesMapper.class,
				Text.class, IntWritable.class, IntSumReducer.class, Text.class,
				IntWritable.class, TextOutputFormat.class, getConf());
		job.setJobName("KnnGridFilesDriver");
		output.getFileSystem(getConf()).delete(output, true);

		job.getConfiguration().set(TOKEN_CASHE, getOption(TOKEN_CASHE));
		job.getConfiguration().set(TRAIN, getOption(TRAIN));
		job.getConfiguration().set(NEIGHBORS, getOption(NEIGHBORS));
		job.getConfiguration().set(DELIMITER, getOption(DELIMITER));
		job.getConfiguration().set(METRIC, getOption(METRIC));
		job.getConfiguration().set(TOKEN_SIZE, getOption(TOKEN_SIZE));
		URI uriTokens = new Path(job.getConfiguration().get(TOKEN_CASHE))
				.toUri();
		DistributedCache.addCacheFile(uriTokens, job.getConfiguration());
		DocumentFabric.cacheTrainData(job.getConfiguration(), TRAIN);

		boolean succeeded = job.waitForCompletion(true);
		return succeeded ? 0 : -1;
	}

}
