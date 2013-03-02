package org.mpei.kmeans.train;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.mpei.data.document.DocumentInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansDriverTrain extends AbstractJob{
	private static final Logger LOG = LoggerFactory
			.getLogger(KMeansDriverTrain.class);

	public int run(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		addInputOption();
		addOutputOption();
		addOption(DefaultOptionCreator.overwriteOption().create());
		Map<String, List<String>> parsedArgs = parseArguments(args);
		if (parsedArgs == null) {
			return -1;
		}
		Path input = getInputPath();
		Path output = getOutputPath();
		if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
			HadoopUtil.delete(getConf(), output);
		}
		Job job = HadoopUtil.prepareJob(input, output, DocumentInputFormat.class,
				KMeansMapperTrain.class, Text.class, MapWritable.class,
				KMeansReducerTrain.class, Text.class, MapWritable.class,
				TextOutputFormat.class, getConf());
		job.setJobName("KMeansDriverTrain");

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);

		int res = job.waitForCompletion(true) ? 0 : 1;
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		return res;
	}

	public static void main(String[] args) {
		String[] debug = {"--input","resources","--output","KMeansDriverTrain","--overwrite"}; 
		try {
			ToolRunner.run(new Configuration(), new KMeansDriverTrain(), debug);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

	}
}
