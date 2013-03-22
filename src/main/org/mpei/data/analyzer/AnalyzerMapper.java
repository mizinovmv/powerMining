package org.mpei.data.analyzer;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mpei.data.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.internal.StringMap;

public class AnalyzerMapper extends Mapper<LongWritable, Document, Text, Text> {
	private static final Logger LOG = LoggerFactory
			.getLogger(AnalyzerMapper.class);

	protected void map(LongWritable key, Document value, Context context)
			throws IOException, InterruptedException {
		context.setStatus(key.toString());
		try {
			StringMap<Double> map = (StringMap<Double>) value.getContext();
			for (Map.Entry<String, Double> entry : map.entrySet()) {
				context.write(new Text(value.getClassName()),
						new Text(entry.getKey()));
			}
		} catch (Exception e) {
			LOG.warn("Can't get coordinates ", e);
		}
	}
}
