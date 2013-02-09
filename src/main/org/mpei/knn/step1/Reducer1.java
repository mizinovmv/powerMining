package org.mpei.knn.step1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;

public class Reducer1<Key> extends Reducer<Key, IntWritable, Text, Text> {

	public void reduce(Key key, Iterable<IntWritable> values, 
                     Context context) throws IOException, InterruptedException {
    int sum = 0;

    for (IntWritable val : values) {
      sum += val.get();
    }  
    JSONObject json = new JSONObject();
    json.put("cell_id",key.toString());
    json.put("sum",sum);
    
    context.write(new Text(json.toJSONString()),null);
  }
}
