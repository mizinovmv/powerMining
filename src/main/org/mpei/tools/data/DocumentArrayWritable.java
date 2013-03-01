package org.mpei.tools.data;

import java.io.DataInput;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.mpei.data.document.Document;
import org.mpei.data.document.DocumentFabric;

public class DocumentArrayWritable extends ArrayWritable implements Writable,
		Iterable<Document> {
	public DocumentArrayWritable() {
		super(Document.class);
	}

	public DocumentArrayWritable(Class<? extends Document> valueClass) {
		super(valueClass);
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

	public void readFields(DataInput in) throws IOException {
		Writable[] values = new Writable[in.readInt()]; // construct values
		for (int i = 0; i < values.length; i++) {
			Writable value = DocumentFabric.newInstance();
			value.readFields(in); // read a value
			values[i] = value; // store it in values
		}
		super.set(values);
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
