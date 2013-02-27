package org.mpei.tools.data;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class DocumentArrayWritable extends ArrayWritable implements
		Iterable<Document> {

	public DocumentArrayWritable() {
		super(DefaultDocument.class);
	}

	public int size() {
		return get().length;
	}

	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		for (Writable value : get()) {
			b.append(value.toString());
			b.append("\n");
		}
		return b.toString();
	}

	public Iterator<Document> iterator() {
		return new Itr();
	}

	private class Itr implements Iterator<Document> {
		/**
		 * Index of element to be returned by subsequent call to next.
		 */
		int cursor = 0;

		/**
		 * Index of element returned by most recent call to next or previous.
		 * Reset to -1 if this element is deleted by a call to remove.
		 */
		int lastRet = -1;

		public boolean hasNext() {
			return cursor != get().length;
		}

		public Document next() {
			try {
				Document next = (Document) get()[cursor];
				lastRet = cursor++;
				return next;
			} catch (IndexOutOfBoundsException e) {
				throw new NoSuchElementException();
			}

		}

		public void remove() {
			if (lastRet == -1)
				throw new IllegalStateException();
			try {
				if (lastRet < cursor)
					cursor--;
				lastRet = -1;
			} catch (IndexOutOfBoundsException e) {
				throw new ConcurrentModificationException();
			}
		}

	}
}
