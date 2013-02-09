package org.mpei.knn.step1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.mpei.json.JsonInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver1 {

	private static final Logger log = LoggerFactory.getLogger(Driver1.class);
	private static Map<String, Long> cells = new HashMap<String, Long>();
	private static JSONParser parser = new JSONParser();
	
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			log.error("Usage: KnnDriver <in> <out>");
			System.exit(2);
		}

		Configuration confCellId = new Configuration();
		confCellId.set("knnmapper.textoutputformat.separator", "_");
		confCellId.set("knnmapper.round", "2");
		// confCellId.set("knnmapper.path.words", jobAnalyzer.getJobName());
		confCellId.set("knnmapper.path.words", "knnAnalyzer");
		confCellId.set("knnmapper.numberNN", "5");

		Job jobCellId = new Job(confCellId, "knnCellId");
		// jobCellId.setJarByClass(KnnDriver.class);
		jobCellId.setOutputKeyClass(Text.class);
		jobCellId.setOutputValueClass(IntWritable.class);
		jobCellId.setMapperClass(Mapper1.class);
		jobCellId.setCombinerClass(IntSumReducer.class);
		jobCellId.setReducerClass(Reducer1.class);

		// JobConf map1 = new JobConf(confCellId);
		// ChainReducer chainR = new ChainReducer();
		// chainR.addMapper(jobCellId, Mapper1.class, LongWritable.class,
		// MapWritable.class, Text.class, IntWritable.class, false,
		// map1);

		jobCellId.setInputFormatClass(JsonInputFormat.class);
		jobCellId.setOutputFormatClass(TextOutputFormat.class);

		ControlledJob controlCellId = new ControlledJob(confCellId);
		controlCellId.setJob(jobCellId);

		FileInputFormat.setInputPaths(jobCellId, new Path(args[0]));
		Path outPath2 = new Path(jobCellId.getJobName());
		FileOutputFormat.setOutputPath(jobCellId, outPath2);
		outPath2.getFileSystem(confCellId).delete(outPath2, true);

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
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.error("Job control error", e);
				// ignore
			}
		}
		
//		int knn = Integer.valueOf(jobCellId.getConfiguration().get("knnmapper.numberNN"));
//		String separator = jobCellId.getConfiguration().get(
//				"knnmapper.textoutputformat.separator");
//		
//		FileSystem fs = FileSystem.get(jobCellId.getConfiguration());
//		FSDataInputStream fsIn = null;
//		String file = jobCellId.getJobName() + "/part-r-00000";
//		Path filePath = new Path(file);
//		BufferedReader buffReader = null;
//		BufferedWriter buffWriter = null;
//		try {
//			fsIn = fs.open(filePath);
//			buffReader = new BufferedReader(new InputStreamReader(fsIn));
//			buffWriter = new BufferedWriter(new FileWriter(jobCellId.getJobName()+"_merge"));
//			String line = null;
//			while ((line = buffReader.readLine()) != null) {
//				JSONObject json = (JSONObject) parser.parse(line);
//				// String tmp = line.split("\t")[0];
//				long sum = (Long)json.get("sum");
//				String cell_id =  json.get("cll_id").toString();
//				cells.put(cell_id, sum);
//				if(sum < knn) {
//					String[] coordinates = cell_id.split(separator);
//				}
//			}
//
//			
//		} catch (Exception e) {
//			log.error(e.getMessage());
//		} finally {
//            try {
//            	buffReader.close();
//    			fs.close();
//                if (buffWriter != null) {
//                	buffWriter.flush();
//                	buffWriter.close();
//                }
//            } catch (Exception e) {
//            	log.error(e.getMessage());
//            }
//		}
		
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
