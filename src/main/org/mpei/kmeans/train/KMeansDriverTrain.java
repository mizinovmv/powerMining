package org.mpei.kmeans.train;

import java.io.IOException;
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
import org.mpei.kmeans.KMeansCombiner;
import org.mpei.knn.KnnDriver;
import org.mpei.tools.data.DocumentInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansDriverTrain {
	private static final Logger LOG = LoggerFactory
			.getLogger(KMeansDriverTrain.class);

	public static int start(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// config a job and start it
		Configuration conf = new Configuration();
		Job job = new Job(conf, "KMeansDriverTrain");
		job.setJarByClass(KMeansDriverTrain.class);

		job.setMapperClass(KMeansMapperTrain.class);
		job.setReducerClass(KMeansReducerTrain.class);

		job.setInputFormatClass(DocumentInputFormat.class);
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
		return res;
	}

	public static void main(String[] args) {
		try {
			System.exit(start(args));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

	}
}
