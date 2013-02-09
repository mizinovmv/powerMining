package org.mpei.knn.step2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mapper2 extends Mapper<LongWritable, MapWritable, Text, Text> {

	private static final Logger log = LoggerFactory.getLogger(Mapper2.class);
	private static JSONParser parser = new JSONParser();
	private static ArrayList<String> words = new ArrayList<String>();

	@Override
	protected void setup(
			Mapper<LongWritable, MapWritable, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		if (!words.isEmpty()) {
			return;
		}
		// String file = "knnAnalyzer/part-r-00000";
		String file = context.getConfiguration().get("knnmapper.path.words");
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path filePath = new Path(file,"part-r-00000");
		FSDataInputStream fsIn = null;
		BufferedReader buffReader = null;
		try {
			fsIn = fs.open(filePath);
			buffReader = new BufferedReader(new InputStreamReader(fsIn));
			int count = 0;
			String line = null;
			while ((line = buffReader.readLine()) != null) {
				JSONObject wordJson = (JSONObject) parser.parse(line);
				words.add(wordJson.get("word").toString());
			}
		} catch (Exception e) {
			log.error("Can't open class words in file", file, e);
		} finally {
			buffReader.close();
			fs.close();
		}

		// String fileCells = context.getConfiguration().get(
		// "knnmapper.path.cells")
		// + "/part-r-00000";
		// Path filePathCells = new Path(fileCells);
		// BufferedReader buffReaderCells = null;
		// try {
		// fsIn = fs.open(filePathCells);
		// buffReaderCells = new BufferedReader(new InputStreamReader(fsIn));
		// String line = null;
		// while ((line = buffReaderCells.readLine()) != null) {
		// JSONObject wordJson = (JSONObject) parser.parse(line);
		// String cell_id = wordJson.get("cell_id").toString();
		// long sum = (Long) wordJson.get("sum");
		// cells.put(cell_id, sum);
		// }
		// } catch (Exception e) {
		// log.error("Can't open class words in file", file, e);
		// } finally {
		// buffReaderCells.close();
		// fs.close();
		// }

		// for (int i = 0; i < words.size(); i++) {
		// log.info(words.get(i));
		// }
		// for (Object key : cells.keySet()) {
		// log.info(key.toString());
		// log.info(cells.get(key).toString());
		// }
		log.info("Setup of KnnMapper done");
	}

	protected void map(LongWritable key, MapWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		context.setStatus(key.toString());

		int delimiter = Integer.valueOf(context.getConfiguration().get(
				"knnmapper.round"));

		ArrayList<Double> pointCoordinates = new ArrayList<Double>(words.size());
		ArrayList<Double> cellCoordinates = new ArrayList<Double>(words.size());
		for (int i = 0; i < words.size(); i++) {
			pointCoordinates.add(0d);
			cellCoordinates.add(0d);
		}
		String doc = null;
		String className = null;
		try {
			doc = value.get(new Text("document")).toString();
			className = value.get(new Text("class")).toString();
			Object tokens = value.get(new Text("tokens"));
			if (tokens == null) {
				return;
			}
			JSONArray coordinates = (JSONArray) parser.parse(tokens.toString());
			for (Object tmp : coordinates) {
				JSONObject coordinate = (JSONObject) tmp;
				String token = coordinate.get("token").toString();
				double frequency = Double.valueOf(coordinate.get("freq")
						.toString());
				int index = 0;

				if ((index = words.indexOf(token)) < 0) {
					log.warn("No index " + token);
					continue;
				}
				double round = BigDecimal.valueOf(frequency)
						.setScale(delimiter, BigDecimal.ROUND_DOWN)
						.doubleValue();
				cellCoordinates.set(index, round);
				pointCoordinates.set(index, frequency);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
		String separator = context.getConfiguration().get(
				"knnmapper.textoutputformat.separator");
		String coord = StringUtils.join(separator, pointCoordinates);
		String cellCoord = StringUtils.join(separator, cellCoordinates);
		// if (cells.containsKey(cellCoord)) {
		JSONObject json = new JSONObject();
		json.put("doc_id", doc);
		json.put("class", className);
		json.put("coordinates", coord);
		context.write(new Text(cellCoord), new Text(json.toJSONString()));
		// }
	}
}
