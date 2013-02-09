package org.mpei.kmeans;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

    protected void reduce(
            Text key,
            java.lang.Iterable<MapWritable> value,
            org.apache.hadoop.mapreduce.Reducer<Text, MapWritable, Text, MapWritable>.Context context)
            throws java.io.IOException, InterruptedException {
        // compute the average center of mass for each cluster
    	MapWritable average = new MapWritable();
        int count = 0;
        for (MapWritable v : value) {
//            int vCount = (int) (float) v.get(MapWritable);
//            v.remove(MapWritable.HEAD);

            if (count == 0) {
                average.putAll(v);
//                count = vCount;
                continue;
            }

//            for (Writable c : v.keySet()) {
//                if (!average.containsKey(c)) {
////                    average.put(c,
////                            v.get(c) / (count + vCount));
//                } else {
//                    average.put(c,
//                            (v.get(c) * vCount + average.get(c) * count)
//                            / (vCount + count));
//                }
//            }
//            count += vCount;
        }
        // output the average
        System.out.println(key + ":\t" + count);
        context.write(key, average);
    }

    ;
}
