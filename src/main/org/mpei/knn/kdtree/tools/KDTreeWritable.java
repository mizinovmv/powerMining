package org.mpei.knn.kdtree.tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.mahout.math.Vector;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

public class KDTreeWritable implements Writable {
	private static final JSONParser parser = new JSONParser();
	private KDTree kdTree;

	public KDTreeWritable() {
	}

	public KDTreeWritable(KDTree tree) {
		this.kdTree = tree;
	}

	public KDTree getKdTree() {
		return kdTree;
	}

	public void setKdTree(KDTree kdTree) {
		this.kdTree = kdTree;
	}

	public static void writeKDTree(DataOutput out, KDTree tree)
			throws IOException {
		KDTreeWritable tr = new KDTreeWritable(tree);
		tr.write(out);
	}

	public static KDTree readKDTree(DataInput in) throws IOException {
		KDTreeWritable tree = new KDTreeWritable();
		tree.readFields(in);
		return tree.getKdTree();
	}

	public void write(DataOutput out) throws IOException {
		Gson gson = new GsonBuilder().registerTypeAdapter(Vector.class,
				new InterfaceAdapter<Vector>()).create();
		WritableUtils.writeCompressedString(out, gson.toJson(kdTree));
	}

	public void readFields(DataInput in) throws IOException {
		Gson gson = new GsonBuilder().registerTypeAdapter(Vector.class,
				new InterfaceAdapter<Vector>()).create();
		KDTree tree = gson.fromJson(WritableUtils.readCompressedString(in),
				KDTree.class);
		this.kdTree = tree;
	}
}
