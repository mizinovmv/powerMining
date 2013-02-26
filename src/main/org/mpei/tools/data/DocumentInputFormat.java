package org.mpei.tools.data;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input format for document text data for classification
 */
public class DocumentInputFormat extends
		FileInputFormat<LongWritable, MapWritable> {

	private static final Logger log = LoggerFactory
			.getLogger(DocumentInputFormat.class);

	@Override
	public RecordReader<LongWritable, MapWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new DocumentRecordReader();
	}

	public static class DocumentRecordReader extends
			RecordReader<LongWritable, MapWritable> {
		
		private LineRecordReader reader = new LineRecordReader();
		private final Text currentLine = new Text();
		private final MapWritable value_ = new MapWritable();
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public MapWritable getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}

	}
}
