package org.mpei.knn.step2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reducer2<Key> extends Reducer<Key, Text, Text,NullWritable> {
	private static JSONParser parser = new JSONParser();
	private static final Logger log = LoggerFactory.getLogger(Reducer2.class);

	public void reduce(Key key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		try {
//			String separator = context.getConfiguration().get(
//					"knnmapper.textoutputformat.separator");
//			int knn = Integer.valueOf(context.getConfiguration().get(
//					"knnmapper.numberNN"));
			JSONObject jsonOut = new JSONObject();
			jsonOut.put("cell_id", key.toString());
			int i =0;
			JSONArray jsonDocs = new JSONArray();
			for (Text val : values) {
				JSONObject json = (JSONObject) parser.parse(val.toString());
				jsonDocs.add(json);
				++i;
				if(i > 1) {
					int a =0;
				}
			}
			jsonOut.put("docs", jsonDocs.toJSONString());
			jsonOut.put("sum",i);
			context.write(new Text(jsonOut.toJSONString()), null);
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	// // Euclidean distance
	// public double distance(String[] v1, String[] v2) {
	// double sum = 0d;
	// for (int i = 0; i < v1.length; ++i) {
	// double v1Freq = Double.valueOf(v1[i]);
	// double v2Freq = Double.valueOf(v2[i]);
	// sum += Math.pow(v1Freq - v2Freq, 2d);
	// }
	// return Math.sqrt(sum);
	// }
	//
	// public void reduce(Key key, Iterable<Text> values, Context context)
	// throws IOException, InterruptedException {
	// try {
	// String separator = context.getConfiguration().get(
	// "knnmapper.textoutputformat.separator");
	// int knn = Integer.valueOf(context.getConfiguration().get(
	// "knnmapper.numberNN"));
	// for (Text val : values) {
	// JSONObject json = (JSONObject) parser.parse(val.toString());
	// String coordinates = json.get("coordinates").toString();
	// String[] coordArray = coordinates.split(separator);
	// TreeMap<Double, String> neighbors = new TreeMap<Double, String>();
	// for (Text other : values) {
	// // other == val! bug?
	// // if (other == val) {
	// // continue;
	// // }
	// JSONObject jsonOther = (JSONObject) parser.parse(other
	// .toString());
	// String docIdOther = jsonOther.get("doc_id").toString();
	// String coordinatesOther = jsonOther.get("coordinates")
	// .toString();
	// String[] coordArrayOther = coordinatesOther
	// .split(separator);
	// double dist = distance(coordArray, coordArrayOther);
	// if (neighbors.size() < knn) {
	// neighbors.put(dist, docIdOther);
	// } else {
	// double last = neighbors.lastKey();
	// if (dist < last) {
	// neighbors.remove(last);
	// neighbors.put(dist, docIdOther);
	// }
	// }
	// }
	// JSONObject jsonOut = new JSONObject();
	// jsonOut.put("doc_id", json.get("doc_id").toString());
	// jsonOut.put("class", json.get("class").toString());
	// jsonOut.put("coordinates", coordinates);
	// jsonOut.put("cell_id", key.toString());
	// JSONObject neighborJson = new JSONObject();
	// for (Map.Entry<Double, String> neighborEntry : neighbors
	// .entrySet()) {
	// neighborJson.put(neighborEntry.getValue(),
	// neighborEntry.getKey());
	// }
	// jsonOut.put("neighbors", neighborJson.toJSONString());
	// context.write(new Text(jsonOut.toJSONString()), null);
	// }
	// } catch (Exception e) {
	// log.error(e.getMessage());
	// e.printStackTrace();
	// }
	// }
};
