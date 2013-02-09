package org.mpei.kmeans;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends
		Mapper<Text, MapWritable, Text, MapWritable> {

//	private Vector<Pair<String, MapWritable>> centroid = new Vector<Pair<String, MapWritable>>();

	@Override
	protected void setup(
			Mapper<Text, MapWritable, Text, MapWritable>.Context context)
			throws java.io.IOException, InterruptedException {
//		String file = context.getConfiguration().get(
//				"org.kmeans.com", "com.txt");
//		// load the center of mass
//		FileSystem fs = FileSystem.get(context.getConfiguration());
//		BufferedReader buffReader = new BufferedReader(new InputStreamReader(
//				fs.open(new Path(file))));
//		String line = buffReader.readLine();
//
//		int count = 0;
//		while (line != null) {
////			Pair<String,MapWritable> v = DataReader.readLine(line);
////			centroid.add(v);
//			line = buffReader.readLine();
//			count++;
//		}
//		buffReader.close();
//		System.out.println("done.");
	}

	protected void map(LongWritable key, Text value,
			Mapper<Text, MapWritable, Text,Text>.Context context)
			throws java.io.IOException, InterruptedException {
		// calculate the distance for each centers of mass with the training
		// data
		context.setStatus(key.toString());

		// find the nearst center of mass
		double min = Double.MAX_VALUE;
		String nearestCluster = "";
//		for (Vector2<String, MapWritable> cluster : centerOfMass) {
//			double d = cluster.getV2().diceDistance(value);
//			if (d < min) {
//				min = d;
//				nearestCluster = cluster.getV1();
//			}
//		}
		// System.out.println(nearestCluster);
		context.write(new Text(nearestCluster), value);
	}

	@Override
	protected void cleanup(
			org.apache.hadoop.mapreduce.Mapper<Text, MapWritable, Text, MapWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		// centerOfMass.close();
	}
}
