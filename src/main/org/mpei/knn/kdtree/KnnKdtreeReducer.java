package org.mpei.knn.kdtree;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.parser.JSONParser;
import org.mpei.knn.step2.Reducer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnnKdtreeReducer<Key> extends
		Reducer<Key, Text, Text, NullWritable> {
	private static JSONParser parser = new JSONParser();
	private static final Logger log = LoggerFactory.getLogger(Reducer2.class);

	public void reduce(Key key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for(Text val : values) {
			context.write(val,null);
		}
		context.write(new Text(key.toString()), null);
	}
}
