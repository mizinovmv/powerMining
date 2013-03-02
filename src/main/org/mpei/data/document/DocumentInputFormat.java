package org.mpei.data.document;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mpei.knn.KnnDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * Input format for document text data for classification
 */
public class DocumentInputFormat extends
		FileInputFormat<LongWritable, Document> {

	private static final Logger LOG = LoggerFactory
			.getLogger(DocumentInputFormat.class);

	@Override
	public RecordReader<LongWritable, Document> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new DocumentRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}

	public static class DocumentRecordReader extends
			RecordReader<LongWritable, Document> {
		private final LineRecordReader reader = new LineRecordReader();
		private final Document doc = DocumentFabric.newInstance();

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);
		}

		@Override
		public synchronized void close() throws IOException {
			reader.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public Document getCurrentValue() throws IOException,
				InterruptedException {
			return doc;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			while (reader.nextKeyValue()) {
				if (jsonToDocument(reader.getCurrentValue(), doc)) {
					return true;
				}
			}
			return false;
		}

		public static boolean jsonToDocument(Text line, Document doc) {
			if (line.getLength() == 0) {
				return false;
			}
			try {
				doc = DocumentFabric.fromJson(line.toString());
			} catch (Exception e) {
				LOG.error(e.getMessage());
				return false;
			}
			return true;
		}
	}
}
