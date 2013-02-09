package org.mpei.knnanalyzer;

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

public class KnnAnalyzerMapper extends Mapper<LongWritable, MapWritable, Text, IntWritable> {
	private static final Logger log = LoggerFactory.getLogger(KnnAnalyzerMapper.class);
	private static final IntWritable one =  new IntWritable(1);
	private static JSONParser parser;
	
	@Override
	protected void setup(
			Mapper<LongWritable, MapWritable, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		parser = new JSONParser();
	}
	
	protected void map(LongWritable key, MapWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		context.setStatus(key.toString());
			try {
				String doc = value.get(new Text("document")).toString();
				JSONArray coordinates = (JSONArray) parser.parse(value.get(
						new Text("tokens")).toString());
				
				for (Object tmp : coordinates) {
					JSONObject coordinate = (JSONObject) tmp;
					String token = coordinate.get("token").toString();
					double frequency = Double.valueOf(coordinate.get("freq")
							.toString());
					context.write(new Text(token), one);
				}
			} catch (Exception e) {
				log.warn("Can't get coordinates ", e);
			}
//		String separator = context.getConfiguration().get("knnmapper.textoutputformat.separator");
//		context.write(new Text(StringUtils.join(separator,cell_coordinates)), one);
	}
}
