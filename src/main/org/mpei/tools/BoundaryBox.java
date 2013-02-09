package org.mpei.tools;


public class BoundaryBox<T> {
	
	private T center;
	private T halfDimension;
	
	
	public boolean containsPoint(T point) {
		if(String.class == point.getClass()) {
		}
		return true;
	};
	
	//пересекает
	public void intersects(BoundaryBox<T> other) {}

	public T getCenter() {
		return center;
	}

	public void setCenter(T center) {
		this.center = center;
	}

	public T getHalfDimension() {
		return halfDimension;
	}

	public void setHalfDimension(T halfDimension) {
		this.halfDimension = halfDimension;
	};
	
}
