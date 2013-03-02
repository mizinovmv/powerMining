package org.mpei.kmeans.train;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.mpei.data.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansMapperTrain extends
		Mapper<LongWritable, Document, Text, MapWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(KMeansMapperTrain.class);

	@Override
	protected void map(LongWritable key, Document value, Context context)
			throws IOException, InterruptedException {
		LOG.info(value.toString());
	}
}
