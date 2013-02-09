package org.mpei.knnanalyzer;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.mpei.json.JsonInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnnAnalyzerDriver {
	
	private static final Logger log = LoggerFactory.getLogger(KnnAnalyzerDriver.class);
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 1) {
			log.error("Usage: KnnAnalyzer <in> <out>");
			System.exit(2);
		}
//		conf.set("knnmapper.textoutputformat.separator", ".");
		Job job = new Job(conf, "knnAnalyzer");
		job.setJarByClass(KnnAnalyzerDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(KnnAnalyzerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(KnnAnalyzerReducer.class);
		
		job.setInputFormatClass(JsonInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outPath = new Path(job.getJobName());
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		// FsShell shell = new FsShell(conf);
		// String cosFile = conf.get("org.niubility.kmeans.com", "com.txt");
		// res = shell.run(new String[] { "-cp", args[1] + "/part*", cosFile });

		Date startTime = new Date();
		log.info("Job started: " + startTime);
		int res = job.waitForCompletion(true) ? 0 : 1;
		Date end_time = new Date();
		log.info("Job ended: " + end_time);
		log.info("The job took " + (end_time.getTime() - startTime.getTime())
				/ 1000 + " seconds.");
		if (res != 0) {
			log.error("Complete with error");
		}
	}
}