package org.mpei.tools;

import java.util.ArrayList;
import java.util.List;

public class Tree<T> {
	
	private Node<T> root;
//	private BoundaryBox boundary;
	
	public Tree(T rootData) {
		root = new Node<T>();
		root.data = rootData;
		root.children = new ArrayList<Node<T>>();
	}

	public static class Node<T> {
		private T data;
		private Node<T> parent;
		private List<Node<T>> children;
	}
	
    public void insert(T data) {
        root = insert(root,data);
    }
    
    private Node insert(Node<T> node,T value) {
        if (node == null) {
        	Node res = new Node();
        	res.data = value;
        	return res;
        }
//        //// if (eq(x, h.x) && eq(y, h.y)) h.value = value;  // duplicate
//        else if ( less(x, h.x) &&  less(y, h.y)) h.SW = insert(h.SW, x, y, value);
//        else if ( less(x, h.x) && !less(y, h.y)) h.NW = insert(h.NW, x, y, value);
//        else if (!less(x, h.x) &&  less(y, h.y)) h.SE = insert(h.SE, x, y, value);
//        else if (!less(x, h.x) && !less(y, h.y)) h.NE = insert(h.NE, x, y, value);
        return new Node();
    }
}
