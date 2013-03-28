package org.mpei.knn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.MinkowskiDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.mpei.data.document.Document;
import org.mpei.data.document.DocumentFabric;
import org.mpei.kmeans.KMeansDriver;
import org.mpei.knn.kdtree.tools.NearestNeighborList;
import org.mpei.tools.Timer;
import org.mpei.tools.data.Dictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.internal.StringMap;

public class KnnMapper extends
		Mapper<LongWritable, Document, Text, IntWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(KnnMapper.class);
	private static final Timer TIMER = new Timer();
	private static final IntWritable ONE = new IntWritable(1);

	private Dictionary dictionary;
	private List<Map.Entry<double[], String>> trainData;

	@Override
	protected void setup(
			Mapper<LongWritable, Document, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		if (null != cacheFiles && cacheFiles.length > 0) {
			dictionary = new Dictionary();
			for (Path cachePath : cacheFiles) {
				if (cachePath.toString().contains(
						context.getConfiguration()
								.get(KnnDriver.TOKEN_CASHE))) {
					String size = context.getConfiguration().get(
							KMeansDriver.TOKEN_SIZE);
					dictionary.loadTokens(cachePath.toUri(),
							Integer.valueOf(size));
				}
			}
			String name = context.getConfiguration().get(KnnDriver.TRAIN);
			for (Path cachePath : cacheFiles) {
				String parentName = cachePath.getParent().getName();
				if (parentName.equals(name)) {
					loadTrainData(cachePath);
				}
			}
		}

		// String file = context.getConfiguration().get(KnnDriver.TOKEN_CASHE);
		// FileSystem fs = FileSystem.get(context.getConfiguration());
		// Path filePath = new Path(file);
		// FSDataInputStream fsIn = null;
		// BufferedReader buffReader = null;
		// try {
		// fsIn = fs.open(filePath);
		// buffReader = new BufferedReader(new InputStreamReader(fsIn));
		// int count = 0;
		// String line = null;
		// while ((line = buffReader.readLine()) != null) {
		// JSONObject wordJson = (JSONObject) parser.parse(line);
		// words.add(wordJson.get("word").toString());
		// }
		// String train = context.getConfiguration().get(KnnDriver.TRAIN);
		// FileStatus[] status = fs.listStatus(new Path(train));
		// for (int i = 0; i < status.length; ++i) {
		// fsIn = fs.open(status[i].getPath());
		// buffReader = new BufferedReader(new InputStreamReader(fsIn));
		// while ((line = buffReader.readLine()) != null) {
		// if (line.equals("")) {
		// continue;
		// }
		// JSONObject json = (JSONObject) parser.parse(line);
		// Object tokens = json.get("tokens");
		// JSONArray coordinates = (JSONArray) parser.parse(tokens
		// .toString());
		// double[] pointCoordinates = new double[words.size()];
		// for (Object tmp : coordinates) {
		// JSONObject coordinate = (JSONObject) tmp;
		// String token = coordinate.get("token").toString();
		// int index = 0;
		// if ((index = words.indexOf(token)) < 0) {
		// LOG.warn("No index " + token);
		// continue;
		// }
		// double frequency = Double.valueOf(coordinate
		// .get("freq").toString());
		// pointCoordinates[index] = frequency;
		// }
		// json.remove("tokens");
		// documents
		// .add(new AbstractMap.SimpleEntry<double[], Object>(
		// pointCoordinates, json));
		// }
		// }
		// } catch (Exception e) {
		// LOG.error(e.getMessage());
		// } finally {
		// buffReader.close();
		// fs.close();
		// }

		LOG.info("Setup of KnnMapper done");
	}

	private void loadTrainData(Path cachePath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				cachePath.toString()));
		try {
			if (trainData == null) {
				trainData = new ArrayList<Map.Entry<double[], String>>();
			} 
			String line = null;
			Document doc = null;
			while ((line = reader.readLine()) != null) {
				doc = DocumentFabric.fromJson(line);
//				StringMap<Double> map = (StringMap<Double>) doc.getContext();
//				List<String> dict = dictionary.getAll();
//				// List<String> dict = dictionary.get(doc.getClassName());
//				// Vector docV = new DenseVector(dictionary.getAll().size());
//				double[] docV = new double[dict.size()];
//				int index = 0;
//				for (Map.Entry<String, Double> entry : map.entrySet()) {
//					if ((index = dict.indexOf(entry.getKey())) < 0) {
//						continue;
//					}
//					docV[index] = entry.getValue();
//				}
				double[] docV = DocumentFabric.getTokensFreq(doc, dictionary);
				trainData.add(new AbstractMap.SimpleEntry<double[], String>(
						docV, doc.getClassName()));
			}
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	protected void map(LongWritable key, Document value, Context context)
			throws java.io.IOException, InterruptedException {
		context.setStatus(key.toString());
		int nn = Integer.valueOf(context.getConfiguration().get(
				KnnDriver.NEIGHBORS));
		double mL = Double.valueOf(context.getConfiguration().get(
				KnnDriver.METRIC));
		
		// StringMap<Double> map = (StringMap<Double>) value.getContext();
		// // List<String> dict = dictionary.getAll();
		// List<String> dict = dictionary.get(value.getClassName());
		// Vector v = new DenseVector(dictionary.getAll().size());
		// for (Map.Entry<String, Double> entry : map.entrySet()) {
		// if (dict.contains(entry.getKey())) {
		// int index = dict.indexOf(entry.getKey());
		// v.set(index, entry.getValue());
		// }
		// }
		// // knn Algorithm
		// DistanceMeasure metric = new MinkowskiDistanceMeasure(mL);
		// String nearest = nearestClass(v, metric, nn);

		NearestNeighborList nnl = new NearestNeighborList(nn);
		DistanceMeasure metric = new MinkowskiDistanceMeasure(mL);
		double[] docV = DocumentFabric.getTokensFreq(value, dictionary);
		Vector v = new DenseVector(docV);
		for (Map.Entry<double[], String> doc : trainData) {
			double priority = metric.distance(v, new DenseVector(doc.getKey()));
			nnl.insert(doc.getValue(), priority);
		}
//		StringMap<Double> map = (StringMap<Double>) value.getContext();
//		Map<String, Vector> cache = new HashMap<String, Vector>();
//		int index = 0;
//		for (Map.Entry<double[], String> doc : trainData) {
//			Vector v = cache.get(doc.getValue());
//			if (v == null) {
////				List<String> dict = dictionary.get(doc.getValue());
//				List<String> dict = dictionary.getAll();
//				v = new DenseVector(dict.size());
//				for (Map.Entry<String, Double> entry : map.entrySet()) {
////					if (dict.contains(entry.getKey())) {
////						int index = dict.indexOf(entry.getKey());
////						v.set(index, entry.getValue());
////					}
//					if((index = dict.indexOf(entry.getKey())) < 0) {
//						continue;
//					}
//					v.set(index, entry.getValue());
//				}
//				
//				v = new DenseVector(docV);
//				cache.put(doc.getValue(), v);
//			}
//			double priority = metric.distance(v, new DenseVector(doc.getKey()));
//			nnl.insert(doc.getValue(), priority);
//		}
		Map<String, Integer> labels = new TreeMap<String, Integer>();
		for (int i = 0; i < nn; ++i) {
			String nbClass = (String) nnl.removeHighest();
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
		String nearest = maxEntry.getKey();

		if (nearest.compareTo(value.getClassName()) == 0) {
			context.write(new Text("true"), ONE);
			// LOG.info("true");
		} else {
			context.write(new Text("false"), ONE);
			// LOG.info("false");
		}

		// double[] pointCoordinates = new double[words.size()];
		// try {
		// String doc = value.get(new Text("document")).toString();
		// String className = value.get(new Text("class")).toString();
		// Object tokens = value.get(new Text("tokens"));
		// if (tokens == null) {
		// return;
		// }
		// JSONArray coordinates = (JSONArray) parser.parse(tokens.toString());
		// for (Object tmp : coordinates) {
		// JSONObject coordinate = (JSONObject) tmp;
		// String token = coordinate.get("token").toString();
		// int index = 0;
		// if ((index = words.indexOf(token)) < 0) {
		// // log.warn("No index " + token);
		// continue;
		// }
		// double frequency = Double.valueOf(coordinate.get("freq")
		// .toString());
		// pointCoordinates[index] = frequency;
		// }
		//
		// } catch (Exception e) {
		// LOG.error(e.getMessage());
		// e.printStackTrace();
		// }

	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		TIMER.output();
	}

	private String nearestClass(Vector target, DistanceMeasure metric, int nn) {
		NearestNeighborList nnl = new NearestNeighborList(nn);

		TIMER.start(String.valueOf(nn));
		for (Map.Entry<double[], String> doc : trainData) {
			double priority = metric.distance(target, new DenseVector(doc.getKey()));
			nnl.insert(doc.getValue(), priority);
		}
		TIMER.stop();
		Map<String, Integer> labels = new TreeMap<String, Integer>();
		for (int i = 0; i < nn; ++i) {
			String nbClass = (String) nnl.removeHighest();
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