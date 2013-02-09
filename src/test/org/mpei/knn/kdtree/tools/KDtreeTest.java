package org.mpei.knn.kdtree.tools;

import static org.junit.Assert.*;

import java.util.Random;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.junit.Test;

public class KDtreeTest {

	@Test
	public void testNextDimensionDimensionalNode() {
		fail("Not yet implemented");
	}

	@Test
	public void testNextDimensionInt() {
		fail("Not yet implemented");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testKDTree() {
		KDTree kdtree1 = new KDTree(0);
		assertTrue("Test init (dm =0)", kdtree1 != null);
		KDTree kdtree2 = new KDTree(10);
		assertTrue("Test init (dm =10)", kdtree2 != null);
		assertTrue("test init dimension", kdtree2.getDimensions() == 10);
		// fail("Not yet implemented");
	}

	@Test
	public void testRemoveAll() {
		fail("Not yet implemented");
	}

	@Test
	public void testParent() {
		fail("Not yet implemented");
	}

	@Test
	public void testInsert() {
		KDTree kdtree1 = new KDTree(4);
		assertTrue("Test init (dm =4)", kdtree1 != null);
		double[] vector1 = {0.1,23,45,-10};
		kdtree1.insert(vector1,"docName1");
		double[] vector2 = {0.1,-23,45,-10};
		kdtree1.insert(vector2,"docName2");
		double[] vector3 = {0.1,-23,5445,-10};
		kdtree1.insert(vector3,"docName3");	
		double[] vector4 = {0.1,23};
		try {
			kdtree1.insert(vector4, "docName4");
			fail("Error dimension size");
		} catch (RuntimeException e) {
			
		} 
		
	}

	@Test
	public void testGetRoot() {
		KDTree kdtree1 = new KDTree(4);
		assertTrue("Test init (dm =4)", kdtree1 != null);
		double[] vector1 = {0.1,23,45,-10};
		kdtree1.insert(vector1,"docName1");
		KDNode root = kdtree1.getRoot();
		assertNotNull(root);
	}

	@Test
	public void testSetRoot() {
		KDTree kdtree1 = new KDTree(4);
		assertTrue("Test init (dm =4)", kdtree1 != null);
		double[] vector1 = {0.1,23,45,-10};
		kdtree1.insert(vector1,"docName1");
		double[] vector2 = {0.1,-23,45,-10};
		kdtree1.insert(vector2,"docName2");
		double[] vector3 = {0.1,-23,5445,-10};
		kdtree1.insert(vector3,"docName3");	
		KDNode root = new KDNode();
		assertTrue("Test init root", root != null);
		kdtree1.setRoot(root);
		assertSame(root, kdtree1.getRoot());
	}

	@Test
	public void testSearch() {
		fail("Not yet implemented");
	}

	@Test
	public void testToString() {
		fail("Not yet implemented");
	}

	@Test
	public void testCount() {
		fail("Not yet implemented");
	}

	@Test
	public void testHeight() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetNumRecursion() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetNumDoubleRecursion() {
		fail("Not yet implemented");
	}

}
