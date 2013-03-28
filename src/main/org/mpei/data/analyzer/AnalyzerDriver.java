package org.mpei.data.analyzer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.mpei.data.document.DocumentInputFormat;
import org.mpei.knnanalyzer.KnnAnalyzerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyzerDriver {
	private static final Logger LOG = LoggerFactory
			.getLogger(KnnAnalyzerDriver.class);

	public static void main(String[] args) throws Exception {
		String[] debug = { "data" };
		run(debug);
	}

	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 1) {
			LOG.error("Usage: Analyzer <in>");
			System.exit(1);
		}
		Job job = new Job(conf, "Analyzer");
		job.setJarByClass(AnalyzerDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(AnalyzerMapper.class);
		job.setReducerClass(AnalyzerReducer.class);
		job.setInputFormatClass(DocumentInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outPath = new Path(job.getJobName());
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);

		long start = System.currentTimeMillis();
		LOG.info("Job started: ".concat(String.valueOf(start)));
		int res = job.waitForCompletion(true) ? 0 : 1;
		long end = System.currentTimeMillis();
		LOG.info("Job time: ".concat(String.valueOf(end - start)));
	}

}
