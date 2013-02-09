package org.mpei.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GenericTreeNode<T> {

	private List<T> data;
	private BoundaryBox<T> boundaryBox;
	private GenericTreeNode<T> parent;
	private List<GenericTreeNode<T>> children;

	public GenericTreeNode() {
		super();
		children = new ArrayList<GenericTreeNode<T>>();
	}

	public GenericTreeNode(List<T> data) {
		this();
		setData(data);
	}

	public List<GenericTreeNode<T>> getChildren() {
		return this.children;
	}

	public void setChildren(List<GenericTreeNode<T>> children) {
		this.children = children;
	}

	public int getNumberOfChildren() {
		return getChildren().size();
	}

	public boolean hasChildren() {
		return (getNumberOfChildren() > 0);
	}

	public void addChild(GenericTreeNode<T> child) {
		child.setParent(this);
		children.add(child);
	}

	public void addChildAt(int index, GenericTreeNode<T> child)
			throws IndexOutOfBoundsException {
		child.setParent(this);
		children.add(index, child);
	}

	public void removeChildren() {
		this.children = new ArrayList<GenericTreeNode<T>>();
	}

	public void removeChildAt(int index) throws IndexOutOfBoundsException {
		children.get(index).setParent(null);
		children.remove(index);
	}

	public GenericTreeNode<T> getChildAt(int index)
			throws IndexOutOfBoundsException {
		return children.get(index);
	}

	public List<T> getData() {
		return this.data;
	}

	public void setData(List<T> data) {
		this.data = data;
	}

	public String toString() {
		return getData().toString();
	}

	public boolean equals(GenericTreeNode<T> node) {
		return node.getData().equals(getData());
	}

	public int hashCode() {
		return getData().hashCode();
	}

	public String toStringVerbose() {
		String stringRepresentation = getData().toString() + ":[";

		for (GenericTreeNode<T> node : getChildren()) {
			stringRepresentation += node.getData().toString() + ", ";
		}

		// Pattern.DOTALL causes ^ and $ to match. Otherwise it won't. It's
		// retarded.
		Pattern pattern = Pattern.compile(", $", Pattern.DOTALL);
		Matcher matcher = pattern.matcher(stringRepresentation);

		stringRepresentation = matcher.replaceFirst("");
		stringRepresentation += "]";

		return stringRepresentation;
	}

	public BoundaryBox<T> getBoundaryBox() {
		return boundaryBox;
	}

	public void setBoundaryBox(BoundaryBox<T> boundaryBox) {
		this.boundaryBox = boundaryBox;
	}

	// True if a Leaf Node
	public boolean isLeaf() {
		return children.size() == 0;
	}

	// True if the Root of the Tree
	public boolean isRoot() {
		return parent == null;
	}

	public GenericTreeNode<T> getParent() {
		return parent;
	}

	public void setParent(GenericTreeNode<T> parent) {
		this.parent = parent;
	}

	// Вставить точку
	public boolean insert(T point)
	{
	// Игнорировать объекты, не принадлежащие дереву
	if (!boundaryBox.containsPoint(point))
	  return false; // Объект не может быть добавлен
	
//	// Если есть место, осуществить вставку
//	if (points.size < QT_NODE_CAPACITY)
//	{
//	  points.append(p);
//	  return true;
//	}
//
//	// Далее необходимо разделить область и добавить точку в какой-либо узел
//	if (northWest != null)
//	  subdivide();
//
//	if (northWest->insert(p)) return true;
//	if (northEast->insert(p)) return true;
//	if (southWest->insert(p)) return true;
//	if (southEast->insert(p)) return true;

	// По каким-то причинам вставка может не осуществиться (чего на самом деле не должно происходить)
	return false;
	}
	
	
	public static void main(String[] args) {

		// I am root!
		// /\
		// A B
		// /\
		// C D
		// \
		// E
		GenericTree<String> tree = new GenericTree<String>();

//		GenericTreeNode<String> root1 = new GenericTreeNode<String>(
//				"0.9_0.9");
//		GenericTreeNode<String> childA = new GenericTreeNode<String>("0.1_0.0");
//		GenericTreeNode<String> childB = new GenericTreeNode<String>("0.2_0.1");
//		GenericTreeNode<String> childC = new GenericTreeNode<String>("0.0_0.1");
//		GenericTreeNode<String> childD = new GenericTreeNode<String>("0.2_0.3");
//		GenericTreeNode<String> childE = new GenericTreeNode<String>("0.1_0.0");
//
//		childD.addChild(childE);
//
//		childB.addChild(childC);
//		childB.addChild(childD);
//
//		root1.addChild(childA);
//		root1.addChild(childB);
//
//		tree.setRoot(root1);

	}
}
