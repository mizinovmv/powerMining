package org.mpei.kmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.MinkowskiDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.codehaus.jettison.json.JSONObject;
import org.jfree.util.Log;
import org.mpei.data.document.Document;
import org.mpei.data.document.DocumentFabric;
import org.mpei.knn.KnnDriver;
import org.mpei.tools.data.Dictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.internal.StringMap;

public class KMeansMapper extends
		Mapper<LongWritable, Document, Text, IntWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(KMeansMapper.class);
	static final IntWritable ONE = new IntWritable(1);
	private Map<String, Vector> means;
	private Dictionary dictionary = new Dictionary();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		if (null != cacheFiles && cacheFiles.length > 0) {
			for (Path cachePath : cacheFiles) {
				if (cachePath.toString().contains(
						context.getConfiguration()
								.get(KMeansDriver.TOKEN_CASHE))) {
					String size = context.getConfiguration().get(
							KMeansDriver.TOKEN_SIZE);
					dictionary.loadTokens(cachePath.toUri(),
							Integer.valueOf(size));
				}
			}
			String name = context.getConfiguration().get(
					KMeansDriver.MEANS_PATH);
			for (Path cachePath : cacheFiles) {
				String parentName = cachePath.getParent().getName();
				if (parentName.equals(name)) {
					loadMeans(cachePath);
				}
			}
		}
	}

	protected void map(LongWritable key, Document value, Context context)
			throws java.io.IOException, InterruptedException {
		// calculate the distance for each centers of mass with the training
		// data
		context.setStatus(key.toString());
		double[] docV = DocumentFabric.getTokensFreq(value, dictionary);
		Vector v = new DenseVector(docV);
		// find the nearst center of mass O(k)
		double mL = Double.valueOf(context.getConfiguration().get(
				KMeansDriver.METRIC));
		double min = Double.MAX_VALUE;
		DistanceMeasure metric = new MinkowskiDistanceMeasure(mL);
		String nearest = null;
		for (Map.Entry<String, Vector> mean : means.entrySet()) {
			double d = metric.distance(v, mean.getValue());
			if (d < min) {
				min = d;
				nearest = mean.getKey();
			}
		}

		if (nearest.compareTo(value.getClassName()) == 0) {
			context.write(new Text("true"), ONE);
			// LOG.info("true");
		} else {
			context.write(new Text("false"), ONE);
			// LOG.info("false");
		}
	}

	void loadMeans(Path cachePath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				cachePath.toString()));
		try {
			if (means != null) {
				return;
			}
			String line = null;
			means = new HashMap<String, Vector>();
			Document mean = null;
			while ((line = reader.readLine()) != null) {
				mean = DocumentFabric.fromJson(line);
				StringMap<Double> map = (StringMap<Double>) mean.getContext();
				Vector meanV = new DenseVector(dictionary.getAll().size());
				for (Map.Entry<String, Double> entry : map.entrySet()) {
					if (dictionary.get(mean.getClassName()).contains(
							entry.getKey())) {
						int index = dictionary.get(mean.getClassName())
								.indexOf(entry.getKey());
						if (index >= dictionary.getAll().size()) {
							continue;
						}
						meanV.set(index, entry.getValue());
					}
				}
				means.put(mean.getClassName(), meanV);
			}
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
}
