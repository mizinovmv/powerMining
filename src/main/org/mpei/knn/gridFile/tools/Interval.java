package org.mpei.knn.gridFile.tools;

public class Interval {

	/*
	 * lower bound
	 */
	private final double lowerBound;

	/*
	 * upper bound
	 */
	private final double upperBound;

	/**
	 * interval [lowerBound,upperBound]
	 * 
	 * 
	 * @param lower
	 * 
	 * @param upper
	 *            upper bound
	 */
	public Interval(double lower, double upper) {
		if (lower > upper) {
			throw new IllegalArgumentException("lower gt upper");
		}
		this.lowerBound = lower;
		this.upperBound = upper;
	}

	/**
	 * Copy constructor
	 * 
	 * @param interv
	 *            other
	 */
	public Interval(Interval interv) {
		this.lowerBound = interv.lowerBound;
		this.upperBound = interv.upperBound;
	}

	/**
	 * Get lowerBound
	 * 
	 * @return lowerBound
	 */
	public double getLowerBound() {
		return lowerBound;
	}

	/**
	 * Get upperBound
	 * 
	 * @return upperBound
	 */
	public double getUpperBound() {
		return upperBound;
	}

	/**
	 * Covers other Interval
	 * 
	 * @param i
	 *            Interval
	 * @return If other Interval is covered return true, else false.
	 */
	public boolean completelyCovers(Interval i) {
		if (this == i) {
			return true;
		}
		if (lowerBound <= i.lowerBound && upperBound >= i.upperBound) {
			return true;
		}
		return false;
	}

	/**
	 * Get length
	 * 
	 * @return length
	 */
	public double getLength() {
		if (length == -1) {
			length = upperBound - lowerBound;
		}
		return length;
	}

	/**
	 * Enlarge by other Interval
	 * 
	 * @param interval
	 *            other
	 * @return result of enlarging
	 */
	public Interval enlargeToCover(Interval interval) {
		if (this == interval) {
			return this;
		}
		if (this.completelyCovers(interval)) {
			return this;
		}
		double newlower = Math.min(this.lowerBound, interval.lowerBound);
		double newupper = Math.max(this.upperBound, interval.upperBound);
		return new Interval(newlower, newupper);
	}

	/**
	 * Union with others Interval.
	 * 
	 * @param itvs
	 *            others
	 * @return result of union
	 */
	public static Interval getUnion(Interval[] itvs) {
		double lower = Double.MAX_VALUE;
		double upper = Double.MIN_VALUE;
		for (int i = 0; i < itvs.length; ++i) {
			if (lower > itvs[i].lowerBound) {
				lower = itvs[i].lowerBound;
			}
			if (upper < itvs[i].upperBound) {
				upper = itvs[i].upperBound;
			}
		}
		return new Interval(lower, upper);
	}

	/**
	 * [lowerBound,upperBound]
	 * 
	 * @return String view
	 */
	@Override
	public String toString() {
		if (string == null) {
			StringBuilder sb = new StringBuilder();
			sb.append('[');
			sb.append(lowerBound);
			sb.append(',');
			sb.append(upperBound);
			sb.append(']');
			string = sb.toString();
		}
		return string;
	}

	/**
	 * 
	 * Equal by bounds
	 * 
	 * @return return true if bounds equals
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Interval)) {
			return false;
		}
		Interval oi = (Interval) o;
		if (oi.upperBound == upperBound && oi.lowerBound == lowerBound) {
			return true;
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (hash == 17) {
			long f = Double.doubleToLongBits(lowerBound);
			hash = 37 * hash + (int) (f ^ (f >>> 32));
			f = Double.doubleToLongBits(upperBound);
			hash = 37 * hash + (int) (f ^ (f >>> 32));
		}
		return hash;
	}

	/*
	 * defaut length
	 */
	private double length = -1;

	/*
	 * defaut hash
	 */
	private int hash = 17;

	/*
	 * defaut String
	 */
	private String string;

}
