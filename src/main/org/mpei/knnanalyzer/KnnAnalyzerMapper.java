package org.mpei.knnanalyzer;

import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.mpei.data.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.internal.StringMap;

public class KnnAnalyzerMapper extends
		Mapper<LongWritable, Document, Text, IntWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(KnnAnalyzerMapper.class);
	private static final IntWritable ONE = new IntWritable(1);

	protected void map(LongWritable key, Document value, Context context)
			throws java.io.IOException, InterruptedException {
		context.setStatus(key.toString());
		try {
			StringMap<Double> map = (StringMap<Double>) value.getContext();
			for (Map.Entry<String, Double> entry : map.entrySet()) {
				context.write(new Text(entry.getKey()), ONE);
			}
		} catch (Exception e) {
			LOG.warn("Can't get coordinates ", e);
		}
	}
}
