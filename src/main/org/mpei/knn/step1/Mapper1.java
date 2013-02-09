package org.mpei.knn.step1;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat.ParseException;

public class Mapper1 extends
		Mapper<LongWritable, MapWritable, Text, IntWritable> {
	private static final Logger log = LoggerFactory.getLogger(Mapper1.class);
	private static JSONParser parser = new JSONParser();
	private static final IntWritable one = new IntWritable(1);

	private static ArrayList<String> words = new ArrayList<String>();

	@Override
	protected void setup(
			Mapper<LongWritable, MapWritable, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		if (!words.isEmpty()) {
			return;
		}
		// String file = "knnAnalyzer/part-r-00000";
		String file = context.getConfiguration().get("knnmapper.path.words")
				+ "/part-r-00000";
		log.info(file);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path filePath = new Path(file);
		FSDataInputStream fsIn = null;
		BufferedReader buffReader = null;
		try {
			fsIn = fs.open(filePath);
			buffReader = new BufferedReader(new InputStreamReader(fsIn));
			String line = null;
			while ((line = buffReader.readLine()) != null) {
				JSONObject wordJson = (JSONObject) parser.parse(line);
				words.add(wordJson.get("word").toString());
			}
			log.info("word size" + Integer.toString(words.size()));
		} catch (Exception e) {
			words.clear();
			log.error(e.getMessage());
			e.printStackTrace();
		} finally {
			buffReader.close();
			fs.close();
		}

		log.info("Setup of KnnMapper done");
	}

	protected void map(LongWritable key, MapWritable value, Context context)
			throws java.io.IOException, InterruptedException {

		int delimiter = Integer.valueOf(context.getConfiguration().get(
				"knnmapper.round"));

		context.setStatus(key.toString());
		ArrayList<Double> cellCoordinates = new ArrayList<Double>(words.size());
		for (int i = 0; i < words.size(); i++) {
			cellCoordinates.add(0d);
		}
		try {
			String doc = value.get(new Text("document")).toString();
			Object tokens = value.get(new Text("tokens"));
			if(tokens == null) {
				return;
			}
			JSONArray coordinates = (JSONArray) parser.parse(tokens.toString());
			for (Object coordinate_ : coordinates) {
				JSONObject coordinate = (JSONObject) coordinate_;
				String token = coordinate.get("token").toString();
				double frequency = Double.valueOf(coordinate.get("freq")
						.toString());
				int index = 0;
				if ((index = words.indexOf(token)) < 0) {
					// log.warn("No index", token);
					continue;
				}
				double round = BigDecimal.valueOf(frequency)
						.setScale(delimiter, BigDecimal.ROUND_DOWN)
						.doubleValue();

				// log.info("token: " + token + " tf: "
				// + Double.toString(frequency) + " round: "
				// + Double.toString(round));
				cellCoordinates.set(index, round);
			}	
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}

		String separator = context.getConfiguration().get(
				"knnmapper.textoutputformat.separator");

		// for (int i = 0; i < cell_coordinates.size(); i++) {
		// log.info(cell_coordinates.get(i));
		// }
		//
		// context.write(new
		// Text(Integer.toString(StringUtils.join(separator,cell_coordinates).hashCode())),
		// one);
		context.write(new Text(StringUtils.join(separator, cellCoordinates)),
				one);
	}
}
