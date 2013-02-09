package org.mpei.knn.kdtree.tools;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Test;

public class KDTreeWritableTest {
	@Test
	public void testKDTreeWritable() {
		KDTree kdTree = new KDTree(10);
		assertNotNull(kdTree);
		KDTreeWritable kdTreeWr = new KDTreeWritable(kdTree);
		assertSame(kdTreeWr.getKdTree(), kdTree);
	}

	@Test
	public void testWrite() throws IOException{
		KDTree kdTree = new KDTree(4);
		double[] vector1 = {0.1,223,45,-11};
		kdTree.insert(vector1,"docName1");
		double[] vector2 = {0.1,-23,345,-10};
		kdTree.insert(vector2,"docName2");
		double[] vector3 = {0.1,-23,5445,-10};
		kdTree.insert(vector3,"docName3");

		DataOutputStream out = new DataOutputStream(new FileOutputStream(
		        "KDTree.bin"));
		KDTreeWritable.writeKDTree(out, kdTree);
		DataInputStream in = new DataInputStream(new FileInputStream(
		        "KDTree.bin"));
		KDTree kdTreeR = KDTreeWritable.readKDTree(in);
		assertSame(kdTree, kdTreeR);
	}
}
