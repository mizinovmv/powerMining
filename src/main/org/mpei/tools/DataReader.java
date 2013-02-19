package org.mpei.tools;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.mpei.knn.KnnDriver;
import org.mpei.json.JsonInputFormat;
import org.mpei.json.JsonLoadWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 	


public class DataReader {
	
	public static class MyMapper extends TableMapper<Text, Text> {

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws InterruptedException, IOException {
			// process data for the row from the Result instance.
		}
	}
	
	private static final Logger log = LoggerFactory.getLogger(KnnDriver.class);

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI( conf, "mongodb://localhost/test.in" );
        MongoConfigUtil.setOutputURI( conf, "mongodb://localhost/test.out" );
        System.out.println( "Conf: " + conf );

        final Job job = new Job( conf, "word count" );
        
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "ExampleReadWrite");
		job.setJarByClass(DataReader.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob("in", // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				null, // mapper output key
				null, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob("out", // output table
				null, // reducer class
				job);
		job.setNumReduceTasks(0);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
	
	// public static void main(String[] args) throws Exception {
	// if (args.length != 2) {
	// log.error("Usage: DataReader <file_name> <out>");
	// System.exit(2);
	// }
	// Configuration conf = new Configuration();
	// FileSystem fs = FileSystem.get(conf);
	//
	// Path inPath = new Path(args[0]);
	// Path outPath = new Path(args[1]);
	//
	// conf.set("datareader.inpath", inPath.toString() + "/");
	// conf.set("datareader.api.class","https://classification-mizinov.rhcloud.com/api/getClasses");
	// conf.set("datareader.api.document","https://classification-mizinov.rhcloud.com/api/getDocuments?class=");
	//
	// JsonLoadWriter.httpGet(fs,conf.get("datareader.api.class"),
	// conf.get("datareader.inpath"));
	//
	// Job job = new Job(conf, "Knn");
	//
	// job.setInputFormatClass(JsonInputFormat.class);
	// job.setOutputFormatClass(TextOutputFormat.class);
	//
	// job.setMapperClass(DataMapper.class);
	//
	// FileInputFormat.setInputPaths(job, inPath);
	// FileOutputFormat.setOutputPath(job, outPath);
	// outPath.getFileSystem(conf).delete(outPath, true);
	//
	// Date startTime = new Date();
	// log.info("Job started: " + startTime);
	// int res = job.waitForCompletion(true) ? 0 : 1;
	// Date end_time = new Date();
	// log.info("Job ended: " + end_time);
	// log.info("The job took " + (end_time.getTime() - startTime.getTime())
	// / 1000 + " seconds.");
	// if (res != 0) {
	// log.error("Complete with error");
	// }
	// }
}


