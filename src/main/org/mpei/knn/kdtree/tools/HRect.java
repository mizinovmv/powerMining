/**
 * %SVN.HEADER%
 * 
 * based on work by Simon Levy
 * http://www.cs.wlu.edu/~levy/software/kd/
 */
package org.mpei.knn.kdtree.tools;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

// Hyper-Rectangle class supporting KDTree class

class HRect {

	protected Vector min;
	protected Vector max;

	protected HRect(int ndims) {
		min = new DenseVector(ndims);
		max = new DenseVector(ndims);
		// min = new HPoint(ndims);
		// max = new HPoint(ndims);
	}

	protected HRect(Vector vmin, Vector vmax) {
		min = (Vector) vmin.clone();
		max = (Vector) vmax.clone();
		// min = (HPoint)vmin.clone();
		// max = (HPoint)vmax.clone();
	}

	protected Object clone() {

		return new HRect(min, max);
	}

	// from Moore's eqn. 6.6
	protected Vector closest(Vector t) {
		// HPoint p = new HPoint(t.coord.length);
		Vector p = new DenseVector(t.size());
		for (int i = 0; i < t.size(); ++i) {
			if (t.get(i) <= min.get(i)) {
				p.set(i, min.get(i));
			} else if (t.get(i) >= max.get(i)) {
				p.set(i, max.get(i));
			} else {
				p.set(i, t.get(i));
			}
		}

		return p;
	}

	// used in initial conditions of KDTree.nearest()
	protected static HRect infiniteHRect(int d) {

		Vector vmin = new DenseVector(d);
		Vector vmax = new DenseVector(d);

		for (int i = 0; i < d; ++i) {
			vmin.set(i,Double.NEGATIVE_INFINITY);
			vmax.set(i,Double.POSITIVE_INFINITY);
		}

		return new HRect(vmin, vmax);
	}

	// currently unused
	protected HRect intersection(HRect r) {

		Vector newmin = new DenseVector(min.size());
		Vector newmax = new DenseVector(min.size());

		for (int i = 0; i < min.size(); ++i) {
			newmin.set(i, Math.max(min.get(i), r.min.get(i)));
			newmax.set(i, Math.max(max.get(i), r.max.get(i)));
//			newmin.coord[i] = Math.max(min.coord[i], r.min.coord[i]);
//			newmax.coord[i] = Math.min(max.coord[i], r.max.coord[i]);
			if (newmin.get(i) >= newmax.get(i))
				return null;
		}

		return new HRect(newmin, newmax);
	}

	// currently unused
	protected double area() {

		double a = 1;

		for (int i = 0; i < min.size(); ++i) {
			a *= (max.get(i) - min.get(i));
		}

		return a;
	}

	public String toString() {
		return min + "\n" + max + "\n";
	}
}
