package org.mpei.kmeans.train;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducerTrain extends
		Reducer<Text, MapWritable, Text, MapWritable> {

}
