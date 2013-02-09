package org.mpei.knn.kdtree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.math.DenseVector;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mpei.knn.KnnMapper;
import org.mpei.knn.kdtree.tools.KDTree;
import org.mpei.knn.kdtree.tools.KDTreeWritable;
import org.mpei.tools.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class KnnKdtreeMapper extends
		Mapper<LongWritable, MapWritable, Text, IntWritable> {
	private static final Logger log = LoggerFactory.getLogger(KnnMapper.class);
	private static final JSONParser parser = new JSONParser();
	private static final Timer timer = new Timer();
	private static final IntWritable one = new IntWritable(1);
	private static final ArrayList<String> words = new ArrayList<String>();

	private static KDTree kdtree = null;

	@Override
	protected void setup(
			Mapper<LongWritable, MapWritable, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		if (!words.isEmpty()) {
			return;
		}
		// String file = "knnAnalyzer/part-r-00000";
		String tokenCashe = context.getConfiguration().get(
				KnnKdtreeDriver.TOKEN_CASHE);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path tokenPath = new Path(tokenCashe);
		// Path kdtreePath = new
		// Path(context.getConfiguration().get("mapred.dir.output"),"KDtree.bin");
		Path kdtreePath = new Path("KDTree.bin");
		BufferedReader buffReader = null;
		try {
			FSDataInputStream fsToken = fs.open(tokenPath);
			buffReader = new BufferedReader(new InputStreamReader(fsToken));
			String line = null;
			while ((line = buffReader.readLine()) != null) {
				JSONObject wordJson = (JSONObject) parser.parse(line);
				words.add(wordJson.get("word").toString());
			}
			fsToken.close();
			FSDataInputStream fsTree = fs.open(kdtreePath);
			kdtree = KDTreeWritable.readKDTree(fsTree);
			fsTree.close();
		} catch (IOException e) {
			log.error("Can't open class words in file", tokenPath);
			log.error(e.getMessage());
			e.printStackTrace();
		} catch (ParseException e) {
			log.error("Can't parse line by JSONParser");
			log.error(e.getMessage());
			e.printStackTrace();
		} finally {
			buffReader.close();
			fs.close();
		}
		log.info("Setup of KnnKdtreeMapper done");
	}

	protected void map(LongWritable key, MapWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		if (kdtree == null) {
			throw new IOException("Can't get KDtree model.");
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
			double[] coordValues = new double[words.size()];
			for (Object tmp : coordinates) {
				JSONObject coordinate = (JSONObject) tmp;
				String token = coordinate.get("token").toString();
				int index = 0;
				if ((index = words.indexOf(token)) < 0) {
					// log.warn("No index " + token);
					continue;
				}
				coordValues[index] = Double.valueOf(coordinate.get("freq")
						.toString());
			}
			int numNN = Integer.valueOf(context.getConfiguration().get(
					KnnKdtreeDriver.NEIGHBORS));
			timer.start(String.valueOf(numNN));
			Object[] nb = kdtree.nearest(coordValues, numNN);
			timer.stop();
			TreeMap<String, Integer> NNLables = new TreeMap<String, Integer>();
			for (Object neighbor : nb) {
				JSONObject jsonObj = (JSONObject) parser.parse(new Gson()
						.toJson(neighbor));
				String nbClass = jsonObj.get("class").toString();
				Integer countNb = NNLables.get(nbClass);
				int count = (countNb == null) ? 0 : countNb.intValue();
				NNLables.put(nbClass, ++count);
			}
			ArrayList<Map.Entry<String, Integer>> NNLablesSort = new ArrayList<Map.Entry<String, Integer>>(
					NNLables.size());
			for (Map.Entry<String, Integer> neighbor : NNLables.entrySet()) {
				NNLablesSort.add(neighbor);
			}
			Collections.sort(NNLablesSort,
					new Comparator<Map.Entry<String, Integer>>() {
						public int compare(Entry<String, Integer> o1,
								Entry<String, Integer> o2) {
							return o2.getValue().compareTo(o1.getValue());
						}
					});
			String nnResult = NNLablesSort.get(0).getKey();
			// log.info(nnResult);
			// log.info(className);
			if (nnResult.equals(className)) {
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

}
