package org.mpei.tools;

import java.io.File;
import java.net.URLEncoder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.parser.JSONParser;
import org.mpei.knn.KnnMapper;
import org.mpei.json.JsonLoadWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMapper extends
		Mapper<LongWritable, MapWritable, LongWritable, MapWritable> {

	private static final Logger log = LoggerFactory.getLogger(KnnMapper.class);
	private static JSONParser parser;

	@Override
	protected void setup(
			Mapper<LongWritable, MapWritable, LongWritable, MapWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		parser = new JSONParser();
	}

	protected void map(LongWritable key, MapWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		context.setStatus(key.toString());
		for (Object value_key : value.keySet()) {
			String class_name = value.get(value_key).toString();
			FileSystem fs = FileSystem.get(context.getConfiguration());

			JsonLoadWriter.httpGet(fs,
					context.getConfiguration().get("datareader.api.document")
							+ URLEncoder.encode(class_name, "UTF-8"), class_name);
			log.info(class_name);

		}

	}
}
