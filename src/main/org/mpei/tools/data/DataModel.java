package org.mpei.tools.data;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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
			// values.put(new Text(label), DocumentArrayWritable());
			values.put(new Text(label), NullWritable.get());
		}
	}

	public static DataModel read(String path) {
		DataModel model = null;
		try {
			FileInputStream fstream = new FileInputStream(path);
			DataInputStream in = new DataInputStream(fstream);
			model = new DataModel();
			model.readFields(in);
			// log.info(model.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return model;
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
	public synchronized void addDocuments(Text label, Document[] docs) {
		DocumentArrayWritable array = new DocumentArrayWritable(docs[0].getClass());
		array.set(docs);
		values.put(label, array);
//		DocumentArrayWritable arrayLabel = (DocumentArrayWritable) values
//				.get(label);
//		arrayLabel.set(docs);
	}

	public String[] getLabels() {
		String[] result = new String[values.keySet().size()];
		int i = 0;
		for (Writable value : values.keySet()) {
			if (value instanceof Text) {
				Text txt = (Text) value;
				result[i] = txt.toString();
			}
			++i;
		}
		return result;
	}

	public DocumentArrayWritable getDocuments(String key) {
		return (DocumentArrayWritable)values.get(new Text(key));
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
		values.readFields(in);
	}

	/**
	 * String description
	 * 
	 */
	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		for (Map.Entry<Writable, Writable> value : values.entrySet()) {
			strBuilder.append(value.getKey().toString());
			strBuilder.append("\n");
			strBuilder.append(value.getValue().toString());
			strBuilder.append("\n");
		}
		return strBuilder.toString();
	}
}
