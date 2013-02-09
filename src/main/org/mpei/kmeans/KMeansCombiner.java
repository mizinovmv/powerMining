package org.mpei.kmeans;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {

    protected void reduce(
            Text key,
            Iterable<MapWritable> value,
            org.apache.hadoop.mapreduce.Reducer<Text, MapWritable, Text, MapWritable>.Context context)
            throws java.io.IOException, InterruptedException {
        // compute the average center of mass for each cluster
    	MapWritable average = new MapWritable();
        int count = 0;
        for (MapWritable v : value) {
            if (count == 0) {
                average.putAll(v);
                count = 1;
                continue;
            }
            for (Writable c : v.keySet()) {
                if (!average.containsKey(c)) {
//                    average.put(c, v.get(c) / (count + 1));
                } else {
//                    average.put(c, (v.get(c) + average.get(c) * count)
//                            / (count + 1));
                }
            }
            count++;
        }
//        average.put(key, (float) count);
        // output the average
        context.write(key, average);
    }
}
