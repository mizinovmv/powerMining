package org.mpei.data.analyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class AnalyzerReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		List<String> list = new ArrayList<String>();
		String str = null;
		for (Text v : value) {
			str = v.toString();
			if(list.contains(str)) {
				continue;
			}
			list.add(str);
		}
		JsonObject obj = new JsonObject();
		JsonArray array = new JsonArray();
		for (String v : list) {
			array.add(new JsonPrimitive(v));
		}
		obj.add("words", array);
		obj.addProperty("className",key.toString());
		context.write(new Text(obj.toString()), new Text());
	}
}
