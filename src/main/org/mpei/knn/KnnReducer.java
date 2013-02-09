package org.mpei.knn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnnReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final Logger log = LoggerFactory.getLogger(KnnReducer.class);
	private IntWritable result = new IntWritable();
	
	protected void reduce(
			Text key,
			Iterable<IntWritable> value,
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		
		 int sum = 0;
	     for (IntWritable val : value) {
	       sum += val.get();
	     }
	     result.set(sum);
	     context.write(key, result);
	}
};
