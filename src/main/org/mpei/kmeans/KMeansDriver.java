package org.mpei.kmeans;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mpei.json.JsonInputFormat;

public class KMeansDriver {
	
	public static void main(String[] args) throws Exception {
		
		// config a job and start it
		Configuration conf = new Configuration();
		Job job = new Job(conf, "KMeans");
		job.setJarByClass(KMeansDriver.class);

		job.setMapperClass(KMeansMapper.class);
		job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);

		job.setInputFormatClass(JsonInputFormat.class);
		// job.setOutputFormatClass(ARFFOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path out = new Path(args[1]);
		FileSystem.get(conf).delete(out, true);
		FileOutputFormat.setOutputPath(job, out);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);

		int res = job.waitForCompletion(true) ? 0 : 1;
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		
		System.exit(res);
	}
}
