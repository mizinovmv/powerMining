package org.mpei.knnanalyzer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnnAnalyzerReducer extends
		Reducer<Text, IntWritable, Text, Text> {

	protected void reduce(
			Text key,
			Iterable<IntWritable> value,
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : value) {
			sum += val.get();
		}
		if(sum < 10 || sum > 200) {
			return;
		}
		JSONObject out = new JSONObject();
		out.put("word", key.toString());
		out.put("sum", sum);
		context.write(new Text(out.toJSONString()), null);
	}
};
