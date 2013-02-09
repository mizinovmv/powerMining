package org.mpei.knn.step3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.json.simple.JSONObject;
import org.mpei.json.JsonInputFormat;
import org.mpei.knn.step2.Driver2;
import org.mpei.knn.step2.Mapper2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver3 {

	private static final Logger log = LoggerFactory.getLogger(Driver2.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			log.error("Usage: KnnDriver <in> <out>");
			System.exit(2);
		}
		int NN = 1;
		Date startTime = null;
		while (NN < 145) {
			Configuration confPoint = new Configuration();
			confPoint.set("knnmapper.textoutputformat.separator", "_");
			confPoint.set("knnmapper.round", "2");
			// confPoint.set("knnmapper.path.words", jobAnalyzer.getJobName());
			confPoint.set("knnmapper.path.words", "knnAnalyzer");
			confPoint.set("knnmapper.path.cells", "knnCellId");
			confPoint.set("knnmapper.path.points", "pointCell");
			confPoint.set("knnmapper.numberNN", Integer.toString(NN));

			Job jobPoint = new Job(confPoint, "pointCl");
			// jobPoint.setJarByClass(KnnDriver.class);
			jobPoint.setOutputKeyClass(Text.class);
			jobPoint.setOutputValueClass(IntWritable.class);

			jobPoint.setMapperClass(Mapper3knnCell.class);
			// job.setCombinerClass(KnnReducer.class);
			jobPoint.setReducerClass(IntSumReducer.class);

			jobPoint.setInputFormatClass(JsonInputFormat.class);
			jobPoint.setOutputFormatClass(TextOutputFormat.class);

			ControlledJob controlCellId = new ControlledJob(confPoint);
			controlCellId.setJob(jobPoint);

			FileInputFormat.setInputPaths(jobPoint, new Path(args[0]));
			Path outPath2 = new Path(jobPoint.getJobName());
			FileOutputFormat.setOutputPath(jobPoint, outPath2);
			outPath2.getFileSystem(confPoint).delete(outPath2, true);

			JobControl jobControl = new JobControl("CellKnn");
			jobControl.addJob(controlCellId);

			// FsShell shell = new FsShell(conf);
			// String cosFile = conf.get("org.niubility.kmeans.com", "com.txt");
			// res = shell.run(new String[] { "-cp", args[1] + "/part*", cosFile
			// });
			startTime = new Date();
			log.info("Job started: " + startTime);
			// int res = job.waitForCompletion(true) ? 0 : 1;

			Thread t = new Thread(jobControl);
			t.start();

			while (!jobControl.allFinished()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					log.error("Job control error", e);
					// ignore
				}
			}
			FileSystem fs = null;
			BufferedReader buffReader = null;
			BufferedWriter buffWriter = null;
			FileWriter fstream = new FileWriter("error.txt",true);
			try {
				fs = FileSystem.get(jobPoint.getConfiguration());
				FSDataInputStream fsIn = fs.open(new Path(outPath2
						+ "/part-r-00000"));
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
				buffWriter.write(String.valueOf(NN) + "\t" +String.valueOf(values[0]/(values[0] + values[1]))+"\n");
			} catch (Exception e) {
				log.error(e.getMessage());
				e.printStackTrace();
			} finally {
				buffReader.close();
				buffWriter.close();
				fs.close();
			}
			++NN;
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
