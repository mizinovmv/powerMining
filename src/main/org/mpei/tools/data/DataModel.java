package org.mpei.tools.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Data model with labels and tokens
 * 
 * @see XmlDataModelBuilder
 */
public class DataModel implements Writable {

	private MapWritable values = new MapWritable();

	/**
	 * Construtor for reading from file
	 */
	public DataModel() {
	}

	/**
	 * add tokens for label
	 * 
	 * @param labels
	 *            labels for model
	 */
	public DataModel(String[] labels) {
		for (String label : labels) {
			values.put(new Text(label),new DocumentArrayWritable());
		}
	}

	/**
	 * add tokens for label
	 * 
	 * @param label
	 *            name of class
	 * @param tokens
	 *            tokens for label
	 */
	public void addDocuments(String label, Document[] docs) {
		Text labelText = new Text(label);
		addDocuments(labelText, docs);
	}

	/**
	 * add tokens for label
	 * 
	 * @param label
	 *            name of class
	 * @param tokens
	 *            tokens for label
	 */
	public void addDocuments(Text label, Document[] docs) {
		DocumentArrayWritable arrayLabel = (DocumentArrayWritable)values.get(label);
		synchronized (values) {
			arrayLabel.set(docs);
		}
	}
	public String[] getLabels() {
		return values.keySet().toArray(new String[0]);
	}
	
	public Writable getDocuments(String key) {
		return values.get(key);
	}
	
	/**
	 * write
	 * 
	 * @param out
	 *            out source
	 */
	public void write(DataOutput out) throws IOException {
		values.write(out);
	}

	/**
	 * read from
	 * 
	 * @param in
	 *            in source
	 */
	public void readFields(DataInput in) throws IOException {
		try {
			values.readFields(in);
		} catch (Exception e) {

		}     
	}
	
	/**
	 * String description
	 *            
	 */
	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		for(Map.Entry<Writable, Writable> value : values.entrySet()) {
			strBuilder.append(value.getKey().toString());
			strBuilder.append("\n");
			strBuilder.append(value.getValue().toString());
			strBuilder.append("\n");
		}
		return strBuilder.toString();
	}
}
