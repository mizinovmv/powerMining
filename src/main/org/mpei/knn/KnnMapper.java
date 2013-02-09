package org.mpei.knn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.StringUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.MinkowskiDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.mpei.knn.kdtree.tools.NearestNeighborList;
import org.mpei.tools.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnnMapper extends
		Mapper<LongWritable, MapWritable, Text, IntWritable> {

	private static final Logger log = LoggerFactory.getLogger(KnnMapper.class);
	private static final JSONParser parser = new JSONParser();
	private static final Timer timer = new Timer();
	private static IntWritable one = new IntWritable(1);

	private static ArrayList<String> words = new ArrayList<String>();
	private static ArrayList<Map.Entry<double[], Object>> documents = new ArrayList<Map.Entry<double[], Object>>();

	@Override
	protected void setup(
			Mapper<LongWritable, MapWritable, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		if (!words.isEmpty()) {
			return;
		}
		String file = context.getConfiguration().get(KnnDriver.TOKEN_CASHE);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path filePath = new Path(file);
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
			String train = context.getConfiguration().get(KnnDriver.TRAIN);
			FileStatus[] status = fs.listStatus(new Path(train));
			for (int i = 0; i < status.length; ++i) {
				fsIn = fs.open(status[i].getPath());
				buffReader = new BufferedReader(new InputStreamReader(fsIn));
				while ((line = buffReader.readLine()) != null) {
					if (line.equals("")) {
						continue;
					}
					JSONObject json = (JSONObject) parser.parse(line);
					Object tokens = json.get("tokens");
					JSONArray coordinates = (JSONArray) parser.parse(tokens
							.toString());
					double[] pointCoordinates = new double[words.size()];
					for (Object tmp : coordinates) {
						JSONObject coordinate = (JSONObject) tmp;
						String token = coordinate.get("token").toString();
						int index = 0;
						if ((index = words.indexOf(token)) < 0) {
							log.warn("No index " + token);
							continue;
						}
						double frequency = Double.valueOf(coordinate
								.get("freq").toString());
						pointCoordinates[index] = frequency;
					}
					json.remove("tokens");
					documents
							.add(new AbstractMap.SimpleEntry<double[], Object>(
									pointCoordinates, json));
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		} finally {
			buffReader.close();
			fs.close();
		}

		log.info("Setup of KnnMapper done");
	}

	protected void map(LongWritable key, MapWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		context.setStatus(key.toString());
		int nn = Integer.valueOf(context.getConfiguration().get(
				KnnDriver.NEIGHBORS));
		double mL = Double.valueOf(context.getConfiguration().get(
				KnnDriver.METRIC));
		double[] pointCoordinates = new double[words.size()];
		try {
			String doc = value.get(new Text("document")).toString();
			String className = value.get(new Text("class")).toString();
			Object tokens = value.get(new Text("tokens"));
			if (tokens == null) {
				return;
			}
			JSONArray coordinates = (JSONArray) parser.parse(tokens.toString());
			for (Object tmp : coordinates) {
				JSONObject coordinate = (JSONObject) tmp;
				String token = coordinate.get("token").toString();
				int index = 0;
				if ((index = words.indexOf(token)) < 0) {
					// log.warn("No index " + token);
					continue;
				}
				double frequency = Double.valueOf(coordinate.get("freq")
						.toString());
				pointCoordinates[index] = frequency;
			}
			DistanceMeasure metric = new MinkowskiDistanceMeasure(mL);
			String nbClass = nearestClass(pointCoordinates, metric, nn);

			if (nbClass.compareTo(className) == 0) {
				context.write(new Text("Success"), one);
			} else {
				context.write(new Text("Error"), one);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}

	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		timer.output();
	}

	private String nearestClass(double[] target, DistanceMeasure metric, int nn) {
		NearestNeighborList nnl = new NearestNeighborList(nn);

		timer.start(String.valueOf(nn));
		for (Map.Entry<double[], Object> doc : documents) {
			double priority = metric.distance(new DenseVector(target),
					new DenseVector(doc.getKey()));
			nnl.insert(doc.getValue(), priority);
		}
		timer.stop();
		TreeMap<String, Integer> labels = new TreeMap<String, Integer>();
		for (int i = 0; i < nn; ++i) {
			JSONObject json = (JSONObject) nnl.removeHighest();
			String nbClass = json.get("class").toString();
			Integer countNb = labels.get(nbClass);
			int count = (countNb == null) ? 0 : countNb.intValue();
			labels.put(nbClass, ++count);
		}
		Map.Entry<String, Integer> maxEntry = null;
		for (Map.Entry<String, Integer> entry : labels.entrySet()) {
			if (maxEntry == null
					|| entry.getValue().compareTo(maxEntry.getValue()) > 0) {
				maxEntry = entry;
			}
		}
		return maxEntry.getKey();
	}
}