package org.mpei.data.analyzer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class AnalyzerReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// dictionary
		JsonObject obj = new JsonObject();
		JsonArray array = new JsonArray();
		for (Text v : value) {
			array.add(new JsonPrimitive(v.toString()));
		}
		obj.add("words", array);
		obj.addProperty("className",key.toString());
		context.write(new Text(obj.toString()), new Text());
	}
}
