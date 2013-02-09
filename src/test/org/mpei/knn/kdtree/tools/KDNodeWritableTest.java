package org.mpei.knn.kdtree.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class KDNodeWritableTest {

	@Test
	public void testWrite() throws IOException {
		KDTree kdTree1 = new KDTree(4);
		double[] vector1 = { 0.1, 223, 45, -11 };
		kdTree1.insert(vector1, "docName1");
		double[] vector2 = { 0.11, -23, 345, -10 };
		kdTree1.insert(vector2, "docName2");
		double[] vector3 = { 0.8, -23, 5445, -10 };
		kdTree1.insert(vector3, "docName3");

		DataOutputStream out = new DataOutputStream(new FileOutputStream(
				"KDTree.bin"));
		
		KDNode node = kdTree1.getRoot();
		Gson gson = new GsonBuilder().registerTypeAdapter(Vector.class, new InterfaceAdapter<Vector>())
                .create();
		WritableUtils.writeCompressedString(out, gson.toJson(kdTree1));
//		BytesWritable bytes = new BytesWritable(.)
//		Text obj = new Text(gson.toJson(kdTree1));
//		obj.write(out);
//		assertNotNull(node);
//		VectorWritable vecWr = new VectorWritable(node.k);
//		vecWr.write(out);
//		ObjectWritable objWr = new ObjectWritable(node.v);
//		objWr.write(out);
//		KDNodeWritable nodeLWr = new KDNodeWritable(node.left);
//		nodeLWr.write(out);
//		KDNodeWritable nodeRWr = new KDNodeWritable(node.right);
//		nodeRWr.write(out);
//		BooleanWritable delWr = new BooleanWritable(node.deleted);
//		delWr.write(out);
	}

	@Test
	public void testReadFields() throws IOException {
		KDTree kdTree1 = new KDTree(4);
		double[] vector1 = { 0.1, 223, 45, -11 };
		kdTree1.insert(vector1, "docName1");
		double[] vector2 = { 0.11, -23, 345, -10 };
		kdTree1.insert(vector2, "docName2");
		double[] vector3 = { 0.8, -23, 5445, -10 };
		kdTree1.insert(vector3, "docName3");

		DataInputStream in = new DataInputStream(new FileInputStream(
				"KDTree.bin"));
		KDNode root = kdTree1.getRoot();
		assertNotNull(root);
		
//		Text obj = new Text();
//		obj.readFields(in);
		Gson gson = new GsonBuilder().registerTypeAdapter(Vector.class, new InterfaceAdapter<Vector>())
                .create();
		KDTree kdTree2 = gson.fromJson(WritableUtils.readCompressedString(in), KDTree.class);
		assertEquals(kdTree1, kdTree2);
	}

}
