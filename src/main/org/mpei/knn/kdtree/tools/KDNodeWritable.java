package org.mpei.knn.kdtree.tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.VectorWritable;

public class KDNodeWritable implements Writable {
	private KDNode node;

	public KDNodeWritable() {
	}

	public KDNodeWritable(KDNode node) {
		this.node = node;
	}

	public void set(KDNode node) {
		this.node = node;
	}

	public KDNode get() {
		return node;
	}

	public static void writeKDNode(DataOutput out, KDNodeWritable node)
			throws IOException {
		node.write(out);
	}

	public static KDNode readKDNode(DataInput in) throws IOException {
		KDNodeWritable node = new KDNodeWritable();
		node.readFields(in);
		return node.get();
	}

	public void write(DataOutput out) throws IOException {
		if (node == null) {
//			NullWritable nullWr = NullWritable.get();
//			byte[] bytes = WritableUtils.toByteArray(nullWr);
			out.writeBytes("\n");
			return;
		}
		VectorWritable vecWr = new VectorWritable(node.k);
		vecWr.write(out);
		ObjectWritable objWr = new ObjectWritable(node.v);
		objWr.write(out);
		KDNodeWritable nodeLWr = new KDNodeWritable(node.left);
		nodeLWr.write(out);
		KDNodeWritable nodeRWr = new KDNodeWritable(node.right);
		nodeRWr.write(out);
		BooleanWritable delWr = new BooleanWritable(node.deleted);
		delWr.write(out);

		// if (node == null) {
		// NullWritable nullWr = NullWritable.get();
		// byte[] bytes = WritableUtils.toByteArray(nullWr);
		// WritableUtils.writeCompressedByteArray(out, bytes);
		// return;
		// }
		// /** Dimensional-coordinate. */
		// VectorWritable vecWr = new VectorWritable(node.k);
		// byte[] bytesVecWr = WritableUtils.toByteArray(vecWr);
		// WritableUtils.writeCompressedByteArray(out, bytesVecWr);
		// ObjectWritable objWr = new ObjectWritable(node.v);
		// byte[] byteObjWr = WritableUtils.toByteArray(objWr);
		// WritableUtils.writeCompressedByteArray(out, byteObjWr);
		// // /** Node below this one. */
		// // // protected DimensionalNode below;
		// KDNodeWritable belowWr = new KDNodeWritable(node.left);
		// byte[] byteBelowWr = WritableUtils.toByteArray(belowWr);
		// WritableUtils.writeCompressedByteArray(out, byteBelowWr);
		// KDNodeWritable aboveWr = new KDNodeWritable(node.left);
		// byte[] byteAboveWr = WritableUtils.toByteArray(aboveWr);
		// WritableUtils.writeCompressedByteArray(out, byteAboveWr);
	}

	public void readFields(DataInput in) throws IOException {
		KDNode node = new KDNode();
		VectorWritable vecWr = new VectorWritable();
		try {
			vecWr.readFields(in);
		} catch (EOFException e) {
			this.node = null;
			return;
		}
		node.k = vecWr.get();
		// JSONObjectWritable objWr = new JSONObjectWritable();
		ObjectWritable objWr = new ObjectWritable();
		try {
			objWr.readFields(in);
			node.v = objWr.get();
		} catch (Exception e) {
			node.v = null;
		}
		KDNodeWritable nodeLWr = new KDNodeWritable();
		nodeLWr.readFields(in);
		node.left = nodeLWr.get();
		KDNodeWritable nodeRWr = new KDNodeWritable();
		nodeRWr.readFields(in);
		node.right = nodeRWr.get();
		BooleanWritable delWr = new BooleanWritable();
		delWr.readFields(in);
		node.deleted = delWr.get();

		// byte[] bytes = WritableUtils.readCompressedByteArray(in);
		// ByteArrayInputStream inStream = new ByteArrayInputStream(bytes);
		// ObjectInputStream inObjStream = new ObjectInputStream(inStream);
		// VectorWritable.readVector(inObjStream);
		// byte[] byteObjWr = WritableUtils.readCompressedByteArray(in);
		// inStream = new ByteArrayInputStream(byteObjWr);
		// Object v = null;
		// try {
		// v = inObjStream.readObject();
		// node.v = v;
		// } catch (ClassNotFoundException e) {
		// e.printStackTrace();
		// }
		// KDNode node = new KDNode();
		// Vector point = null;
		// try {
		// point = VectorWritable.readVector(in);
		// } catch (IOException e) {
		// if (point == null) {
		// // there is no point in this node
		// NullWritable.get().readFields(in);
		// return;
		// }
		// }
		// int dimension = in.readInt();
		//
		// DimensionalNode node = new DimensionalNode(dimension, point);
		// node.setBelow(DimensionalNodeWritable.readDimensionalNode(in));
		// node.setAbove(DimensionalNodeWritable.readDimensionalNode(in));
		this.node = node;
	}
}
