package org.mpei.knn.step3;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mapper3knnCell extends
		Mapper<LongWritable, MapWritable, Text, IntWritable> {

	private static final Logger log = LoggerFactory.getLogger(Mapper3.class);
	private static JSONParser parser = new JSONParser();
	private static final IntWritable one = new IntWritable(1);

	private static ArrayList<String> words = new ArrayList<String>();
	private static Map<String, Long> cells = new HashMap<String, Long>();
	private static Map<String, String> pointCells = new HashMap<String, String>();

	@Override
	protected void setup(
			Mapper<LongWritable, MapWritable, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		if (!words.isEmpty() || !pointCells.isEmpty() || !cells.isEmpty()) {
			return;
		}

		// TODO there is complete class in hadoop for reading files
		// String file = "knnAnalyzer/part-r-00000";
		String fileWords = context.getConfiguration().get(
				"knnmapper.path.words")
				+ "/part-r-00000";
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path filePathWords = new Path(fileWords);
		String pointCell = context.getConfiguration().get(
				"knnmapper.path.points")
				+ "/part-r-00000";
		Path filePathPointCell = new Path(pointCell);
		FSDataInputStream fsIn = null;
		BufferedReader buffReader = null;
		try {
			fsIn = fs.open(filePathWords);
			buffReader = new BufferedReader(new InputStreamReader(fsIn));
			int count = 0;
			String line = null;
			while ((line = buffReader.readLine()) != null) {
				JSONObject wordJson = (JSONObject) parser.parse(line);
				words.add(wordJson.get("word").toString());
			}
			fsIn = fs.open(filePathPointCell);
			BufferedReader buffReaderPointCell = new BufferedReader(
					new InputStreamReader(fsIn));
			while ((line = buffReaderPointCell.readLine()) != null) {
				JSONObject json = (JSONObject) parser.parse(line);
				String cell_id = json.get("cell_id").toString();
				String docs = json.get("docs").toString();
				long sum = (Long) json.get("sum");
				pointCells.put(cell_id, docs);
				cells.put(cell_id, sum);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		} finally {
			buffReader.close();
			fs.close();
		}

		// for (int i = 0; i < words.size(); i++) {
		// log.info(words.get(i));
		// }
		// for (Object key : pointCells.keySet()) {
		// log.info(key.toString());
		// log.info(pointCells.get(key).toString());
		// }

		log.info("Setup of KnnMapper done");
	}

	protected void map(LongWritable key, MapWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		context.setStatus(key.toString());

		int delimiter = Integer.valueOf(context.getConfiguration().get(
				"knnmapper.round"));

		ArrayList<Double> pointCoordinates = new ArrayList<Double>(words.size());
		ArrayList<Double> pointCellCoordinates = new ArrayList<Double>(
				words.size());
		for (int i = 0; i < words.size(); i++) {
			pointCoordinates.add(0d);
			pointCellCoordinates.add(0d);
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
					// log.warn("No index " + token);
					continue;
				}
				double round = BigDecimal.valueOf(frequency)
						.setScale(delimiter, BigDecimal.ROUND_DOWN)
						.doubleValue();
				pointCellCoordinates.set(index, round);
				pointCoordinates.set(index, frequency);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}

		String separator = context.getConfiguration().get(
				"knnmapper.textoutputformat.separator");
		int knn = Integer.valueOf(context.getConfiguration().get(
				"knnmapper.numberNN"));

		// knnCell
		Map<String, Integer> NN = new TreeMap<String, Integer>();

		ArrayList<String> NNCell = new ArrayList<String>();

		// String coord = StringUtils.join(separator, pointCoordinates);
		// String cellCoord = StringUtils.join(separator, pointCellCoordinates);

		double range = 0;
		double step = 0;
		double dStep = 0;
		while ((dStep = step / 10) < 1) {
			double st = BigDecimal.valueOf(dStep)
					.setScale(delimiter, BigDecimal.ROUND_DOWN).doubleValue();
			long numNN = 0;
			for (String cell : cells.keySet()) {
				// log.info("cellCoord" + cellCoord);
				String[] coordinates = cell.split(separator);
				BitSet bitMask = new BitSet(coordinates.length);
				// TODO make LSH
				for (int i = 0; i < coordinates.length; ++i) {
					double coordinateValue = Double.valueOf(coordinates[i]);
					double leftBound = coordinateValue - st;
					double rightBound = coordinateValue + st;
					if (leftBound < 0) {
						leftBound = 0;
					}
					if (rightBound > 1) {
						rightBound = 1;
					}
					if (pointCellCoordinates.get(i) < leftBound
							|| pointCellCoordinates.get(i) > rightBound) {
						break;
					}
					bitMask.set(i);
				}
				if (bitMask.cardinality() != coordinates.length) {
					continue;
				}
				if (cells.containsKey(cell)) {
					numNN += cells.get(cell);
					NNCell.add(cell);
					// log.info("cell" + cell);
				}

			}
			if (numNN >= knn) {
				try {
					// log.info("numNN" + String.valueOf(numNN));
					for (String nnCell : NNCell) {
						String docs = pointCells.get(nnCell);
						JSONArray jsonDocs = (JSONArray) parser.parse(docs);
						String doc_id = null;
						String docClass = null;
						String docCoordinates = null;
						for (Object tmp : jsonDocs) {
							JSONObject jsonDoc = (JSONObject) tmp;
							doc_id = jsonDoc.get("doc_id").toString();
							docClass = jsonDoc.get("class").toString();
							// knnCell
							Integer countNb = NN.get(docClass);
							int count = (countNb == null) ? 0 : countNb.intValue();
							NN.put(docClass, ++count);
//							if (NN.containsKey(docClass)) {
//								int count = NN.get(docClass);
//								++count;
//								NN.put(docClass, count);
//							} else {
//								NN.put(docClass, 0);
//							}

						}
					}
					JSONObject jsonOut = new JSONObject();
					jsonOut.put("doc_id", doc);
					jsonOut.put("class", className);

					// knnCell
					Map.Entry<String, Integer> maxEntry = null;
					for (Map.Entry<String, Integer> entry : NN.entrySet()) {
						if (maxEntry == null
								|| entry.getValue().compareTo(
										maxEntry.getValue()) > 0) {
							maxEntry = entry;
						}
					}

					if (maxEntry.getKey().compareTo(className) == 0) {
						context.write(new Text("Success"), one);
					} else {
						context.write(new Text("Error"), one);
					}

					break;
				} catch (Exception e) {
					log.error(e.getMessage());
					e.printStackTrace();
					break;
				}
			} else {
				NNCell.clear();
			}
			++step;
		}
	}

}
