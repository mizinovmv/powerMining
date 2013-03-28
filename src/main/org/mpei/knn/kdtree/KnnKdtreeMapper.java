package org.mpei.knn.kdtree;

import java.io.BufferedReader;
import java.io.FileReader;
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

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.MinkowskiDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mpei.data.document.Document;
import org.mpei.data.document.DocumentFabric;
import org.mpei.kmeans.KMeansDriver;
import org.mpei.knn.KnnDriver;
import org.mpei.knn.KnnMapper;
import org.mpei.knn.kdtree.tools.KDTree;
import org.mpei.knn.kdtree.tools.KDTreeWritable;
import org.mpei.knn.kdtree.tools.NearestNeighborList;
import org.mpei.tools.Timer;
import org.mpei.tools.data.Dictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.internal.StringMap;

public class KnnKdtreeMapper extends
		Mapper<LongWritable, Document, Text, IntWritable> {
	private static final Logger LOG = LoggerFactory.getLogger(KnnMapper.class);
	private static final Timer TIMER = new Timer();
	private static final IntWritable ONE = new IntWritable(1);

	private static Dictionary dictionary;
	private static KDTree kdTree;

	@Override
	protected void setup(
			Mapper<LongWritable, Document, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		if (dictionary != null) {
			return;
		}

		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		if (null != cacheFiles && cacheFiles.length > 0) {
			dictionary = new Dictionary();
			for (Path cachePath : cacheFiles) {
				if (cachePath.toString().contains(
						context.getConfiguration().get(
								KnnKdtreeDriver.TOKEN_CASHE))) {
					String size = context.getConfiguration().get(
							KMeansDriver.TOKEN_SIZE);
					dictionary.loadTokens(cachePath.toUri(),
							Integer.valueOf(size));
				}
			}
			String name = context.getConfiguration().get(KnnKdtreeDriver.TRAIN);
			for (Path cachePath : cacheFiles) {
				String parentName = cachePath.getParent().getName();
				if (parentName.equals(name)) {
					loadTreeData(cachePath);
				}
			}
		}
		LOG.info("Setup of KnnKdtreeMapper done");
	}

	private void loadTreeData(Path cachePath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				cachePath.toString()));
		if (kdTree == null) {
			kdTree = new KDTree(dictionary.getAll().size());
		}
		try {
			String line = null;
			Document doc = null;
			while ((line = reader.readLine()) != null) {
				doc = DocumentFabric.fromJson(line);
				double[] coordValues = DocumentFabric.getTokensFreq(doc,
						dictionary);
				kdTree.insert(coordValues, doc.getClassName());
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
				KnnKdtreeDriver.NEIGHBORS));

		double[] v = DocumentFabric.getTokensFreq(value, dictionary);
		Object[] nb = kdTree.nearest(v, nn);

		Map<String, Integer> labels = new TreeMap<String, Integer>();
		String nbClass = null;
		for (Object neighbor : nb) {
			if (neighbor instanceof String) {
				nbClass = (String) neighbor;
				Integer countNb = labels.get(nbClass);
				int count = (countNb == null) ? 0 : countNb.intValue();
				labels.put(nbClass, ++count);
			} else {
				throw new RuntimeException("Wrong algorithm");
			}
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
	}

//	protected void cleanup(Context context) throws IOException,
//			InterruptedException {
//		TIMER.output();
//	}

}
