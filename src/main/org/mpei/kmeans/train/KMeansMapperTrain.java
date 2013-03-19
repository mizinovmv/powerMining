package org.mpei.kmeans.train;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.mpei.data.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class KMeansMapperTrain extends
		Mapper<LongWritable, Document, Text, MapWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(KMeansMapperTrain.class);
	private static final JsonParser parser = new JsonParser();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

	}

	/**
	 * Sort documents by label of class
	 */
	@Override
	protected void map(LongWritable key, Document value, Context context)
			throws IOException, InterruptedException {
		MapWritable map = new MapWritable();
		String v = value.getContext().toString();
		JsonObject obj = null;
		try {
			JsonElement jelement = parser.parse(v);
			obj = jelement.getAsJsonObject();
		} catch (Exception e) {
			LOG.error(e.getMessage());
			return;
		}
		for (Entry<String, JsonElement> elem : obj.entrySet()) {
			map.put(new Text(elem.getKey()),
					new DoubleWritable(Double.valueOf(elem.getValue()
							.toString())));
		}
		context.write(new Text(value.getClassName()), map);
	}
}
