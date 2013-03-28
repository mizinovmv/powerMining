package org.mpei.knn.gridFile.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

public class GridFile<T> {
	private Map<HRect, List<T>> values = new HashMap<HRect, List<T>>();
	private Map<HRect, List<double[]>> cells = new HashMap<HRect, List<double[]>>();
	private int dimension;
	private double step;

	public GridFile(int dimension, Interval e, int count) {
		this.dimension = dimension;
		step = e.getLength() / count;
		// values.put(hr, new ArrayList<T>());
	}

	private HRect hrInstance(double low, double high) {
		Vector vmin = new DenseVector(dimension);
		Vector vmax = new DenseVector(dimension);
		for (int j = 0; j < dimension; ++j) {
			vmin.set(j, low);
			vmax.set(j, high);
		}
		return new HRect(vmin, vmax);
	}

	/**
	 * Sets the cell content at the specified coordinates
	 * 
	 * @param v
	 *            the coordinates
	 * @param element
	 *            the data element.
	 * @return the previous element stored at that location, which may be null.
	 */
	public boolean add(double[] v, T element) {
		if (v.length != dimension) {
			throw new IllegalArgumentException(
					"Coordinates must have dimension " + dimension);
		}
		HRect result = null;
		for (HRect hr : values.keySet()) {
			if (hr.contains(v)) {
				result = hr;
				break;
			}
		}
		if (result == null) {
			Vector vmin = new DenseVector(dimension);
			Vector vmax = new DenseVector(dimension);
			for (int j = 0; j < dimension; ++j) {
				vmin.set(j, v[j]);
				vmax.set(j, v[j] + step);
			}
			result = new HRect(vmin, vmax);
		}
		List<T> l = values.get(result);
		if (l == null) {
			l = new ArrayList<T>();
		}
		l.add(element);
		values.put(result, l);
		return true;
	}

	/**
	 * Gets the cell at the specified coordinates. The dimensionality of the
	 * coordinate array should be the same as the dimensionality of the grid.
	 * Otherwise an exception is thrown.
	 * 
	 * @param v
	 *            the coordinate for the desired cell
	 * @param nn
	 *            neighbors count
	 * @return the cell content
	 */
	public List<T> get(double[] v, int nn) {
		List<T> l = new ArrayList<T>();
		Vector vmin = new DenseVector(dimension);
		Vector vmax = new DenseVector(dimension);
		for (int j = 0; j < dimension; ++j) {
			vmin.set(j, v[j] - step);
			vmax.set(j, v[j] + step);
		}

		while (l.size() < nn) {
			HRect srch = new HRect(vmin, vmax);
			for (HRect hr : values.keySet()) {
				if (srch.containsHrect(hr)) {
					if (l.containsAll(values.get(hr))) {
						continue;
					} else {
						l.addAll(values.get(hr));
					}
				}
			}
			for (int j = 0; j < dimension; ++j) {
				vmin.set(j, vmin.get(j) - step);
				vmax.set(j, vmax.get(j) + step);
			}
		}
		return l;
	}

	/**
	 * Get the number of non-null values that have been set in the grid
	 * 
	 * @return the number of values.
	 */
	public int getNumValues() {
		return values.size();
	}

	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("{");
		for (HRect hr : values.keySet()) {
			b.append(hr.toString());
			b.append("\n");
		}
		b.append("}");
		return b.toString();
	}

	// public T get(double[] coords) {
	// // TODO Auto-generated method stub
	// return null;
	// }
	//
	// /**
	// * Gets the cell at the specified coordinates. The dimensionality of the
	// * coordinate array should be the same as the dimensionality of the grid.
	// * Otherwise an exception is thrown.
	// *
	// * @param coords
	// * the coordinate for the desired cell
	// * @return the cell content
	// */
	// public List<T> get(int[] coords) {
	// double[] dblCoords = new double[dimCount];
	// return get(dblCoords);
	// }
	//
	// /**
	// * Return the collection of all values contained in the grid
	// *
	// * @return the collection of all values in the grid
	// */
	// public Collection<List<T>> getAllValues() {
	// return valueMap.values();
	// }
	//
	// /**
	// * Get the extent of the coordinate axes for all values within this grid.
	// * This method returns the minimum and maximum values for each coordinate.
	// * It should only return coordinate ranges for the values that have been
	// * set.
	// *
	// * @return an array of values indicating the coordinate extents. The array
	// * is twice the size of the dimension count, and contains values in
	// * the following order: minCoord1, maxCoord1, minCoord2, maxCoord2,
	// * etc.
	// */
	// public double[] getCoordRanges() {
	// double[] ranges = new double[2 * dimCount];
	// for (int i = 0; i < dimCount; i++) {
	// ranges[i] = Double.NaN;
	// }
	// for (HRect coords : valueMap.keySet()) {
	// for (int i = 0; i < dimCount; i++) {
	// if (ranges[2 * i] == Double.NaN
	// || ranges[2 * i] > coords.min.get(i)) {
	// ranges[2 * i] = coords.min.get(i);
	// }
	// if (ranges[2 * i + 1] == Double.NaN
	// || ranges[2 * i + 1] < coords.max.get(i)) {
	// ranges[2 * i + 1] = coords.max.get(i);
	// }
	// }
	// }
	// return ranges;
	// }
	//
	// /**
	// * Get the set of coordinates that have non-null values
	// *
	// * @return the set of coordinates that have non-null values
	// */
	// public Set<double[]> getCoordsWithValues() {
	// return null;
	// }
	//
	// /**
	// * Gets the dimension count.
	// *
	// * @return the dimensionCount
	// */
	// public int getDimensionCount() {
	// return dimCount;
	// }
	//
	// /**
	// * Get the number of non-null values that have been set in the grid
	// *
	// * @return the number of values.
	// */
	// public int getNumValues() {
	// return valueMap.size();
	// }
	//
	// /**
	// * Return whether the grid values should be considered continuous. The
	// * RealGrid is a continuous grid.
	// *
	// * @return true
	// */
	// public boolean isContinuous() {
	// return true;
	// }
	//
	// /**
	// * Sets the cell content at the specified coordinates
	// *
	// * @param coords
	// * the coordinates
	// * @param element
	// * the data element.
	// * @return the previous element stored at that location, which may be
	// null.
	// */
	// public List<T> set(double[] coords, T element) {
	// if (coords.length != dimCount) {
	// throw new IllegalArgumentException(
	// "Coordinates must have dimension " + dimCount);
	// }
	// for (HRect hr : valueMap.keySet()) {
	// if (hr.add(coords)) {
	// List<T> list = valueMap.get(coords);
	// if (list == null) {
	// list = new ArrayList<T>();
	// }
	// list.add(element);
	// return valueMap.put(hr, list);
	// }
	// }
	// return null;
	// }
	//
	// /**
	// * Sets the cell content at the specified coordinates
	// *
	// * @param coords
	// * the coordinates
	// * @param element
	// * the data element.
	// * @return the previous element stored at that location, which may be
	// null.
	// */
	// public List<T> set(int[] coords, T element) {
	// double[] dblCoords = toDouble(coords);
	// return set(dblCoords, element);
	// }
	//
	// /**
	// * Convert the integer coordinates to double coordinates
	// *
	// * @param coords
	// * the integer coordinates
	// * @return the double coordinates
	// */
	// private double[] toDouble(int[] coords) {
	// double[] dblCoords = new double[dimCount];
	// for (int i = 0; i < dimCount; i++) {
	// dblCoords[i] = coords[i];
	// }
	// return dblCoords;
	// }
	//
	// public List<T> set(double[] coords, List<T> element) {
	// for (T e : element) {
	// set(coords, e);
	// }
	// return valueMap.get(coords);
	// }
	//
	// public List<T> set(int[] coords, List<T> element) {
	// double[] dblCoords = toDouble(coords);
	// return set(dblCoords, element);
	// }
	//
	// public void union(int nn) {
	// for (Map.Entry<double[], List<T>> entry : valueMap.entrySet()) {
	// if (entry.getValue().size() < nn) {
	// System.out.println("falseNN");
	// }
	// }
	// }
}
