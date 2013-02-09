package org.mpei.knn.step2;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.mpei.json.JsonInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver2 {

	private static final Logger log = LoggerFactory.getLogger(Driver2.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			log.error("Usage: KnnDriver <in> ");
			System.exit(2);
		}
		
		Configuration confPointCell = new Configuration();
		confPointCell.set("knnmapper.textoutputformat.separator", "_");
		confPointCell.set("knnmapper.round", "2");
//		confPointCell.set("knnmapper.path.words", jobAnalyzer.getJobName());
		confPointCell.set("knnmapper.path.words", "knnAnalyzer");
		confPointCell.set("knnmapper.path.cells","knnCellId");
		confPointCell.set("knnmapper.numberNN","5");
		
		Job jobCellId = new Job(confPointCell, "pointCell");
//		jobCellId.setJarByClass(KnnDriver.class);
		jobCellId.setOutputKeyClass(Text.class);
		jobCellId.setOutputValueClass(Text.class);

		jobCellId.setMapperClass(Mapper2.class);
		jobCellId.setReducerClass(Reducer2.class);

		jobCellId.setInputFormatClass(JsonInputFormat.class);
		jobCellId.setOutputFormatClass(TextOutputFormat.class);

		ControlledJob controlCellId = new ControlledJob(confPointCell);
		controlCellId.setJob(jobCellId);

		FileInputFormat.setInputPaths(jobCellId, new Path(args[0]));
		Path outPath2 = new Path(jobCellId.getJobName());
		FileOutputFormat.setOutputPath(jobCellId, outPath2);
		outPath2.getFileSystem(confPointCell).delete(outPath2, true);

		JobControl jobControl = new JobControl("CellKnn");
		jobControl.addJob(controlCellId);
		
		// FsShell shell = new FsShell(conf);
		// String cosFile = conf.get("org.niubility.kmeans.com", "com.txt");
		// res = shell.run(new String[] { "-cp", args[1] + "/part*", cosFile });
		Date startTime = new Date();
		log.info("Job started: " + startTime);
		// int res = job.waitForCompletion(true) ? 0 : 1;

		Thread t = new Thread(jobControl);
		t.start();

		while (!jobControl.allFinished()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				log.error("Job control error" ,e);
				// ignore
			}
		}
			
		Date end_time = new Date();
		log.info("Job ended: " + end_time);
		log.info("The job took " + (end_time.getTime() - startTime.getTime())
				/ 1000 + " seconds.");
		// if (res != 0) {
		// log.error("Complete with error");
		// }
		System.exit(0);
	}
}
