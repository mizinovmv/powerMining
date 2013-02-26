package org.mpei.tools.data;

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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.mpei.knn.KnnDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input format for document text data for classification
 */
public class DocumentInputFormat extends InputFormat<Text, MapWritable> {

	private static final Logger LOG = LoggerFactory
			.getLogger(DocumentInputFormat.class);

	@Override
	public RecordReader<Text, MapWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new DocumentRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException,
			InterruptedException {
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		String fileName = job.getConfiguration().get("mapred.input.model");
		if (fileName == null) {
			throw new IOException("No model paths specified in job");
		}
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = fs.listStatus(new Path(fileName));
		for (FileStatus file : status) {
			WeakReference<DataModel> modelWeak = new WeakReference<DataModel>(
					DataModel.read(file.getPath().toString()));

			// long length = file.getLen();
			// BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
			// length);
			// if (length != 0) {
			// long blockSize = file.getBlockSize();
			// long splitSize = Math.max(minSize, Math.min(maxSize, blockSize));
			// splits.add(new DocumentSplit());
			// }
		}
		return splits;
	}

	public static class DocumentRecordReader extends
			RecordReader<Text, MapWritable> {

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
		public Text getCurrentKey() throws IOException, InterruptedException {
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
