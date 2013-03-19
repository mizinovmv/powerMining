package org.mpei.kmeans.train;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.mpei.data.document.Document;
import org.mpei.data.document.DocumentFabric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class KMeansReducerTrain extends
		Reducer<Text, MapWritable, Text, NullWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(KMeansReducerTrain.class);
	private final String tag = "centroid ";

	protected void reduce(Text key, Iterable<MapWritable> values,
			Context context) throws IOException, InterruptedException {
		Map<String, Double> result = new HashMap<String, Double>();
		int count = 0;
		for (MapWritable value : values) {
			for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
				double d = Double.valueOf(entry.getValue().toString());
				Double valueWr = result.get(entry.getKey().toString());
				double valueD = (valueWr == null) ? 0 : valueWr;
				result.put(entry.getKey().toString(), d+valueD);
			}
			++count;
		}
		for (Map.Entry<String, Double> value : result.entrySet()) {
			double d = value.getValue();
			value.setValue(d / count);
		}

		Document doc = DocumentFabric.newInstance();
		doc.setName(tag + key.toString());
		doc.setContext(result);
		doc.setClassName(key.toString());
		context.write(new Text(DocumentFabric.toJson(doc)), NullWritable.get());
	}
}
