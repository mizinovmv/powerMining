package org.mpei.tools.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class DocumentSplit extends InputSplit implements Writable {
	private Document document;
	private long start;
	private long length;
	private String[] hosts;

	public DocumentSplit() {
	}
		
	@Override
	public long getLength() throws IOException, InterruptedException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		if (this.hosts == null) {
			return new String[] {};
		} else {
			return this.hosts;
		}
	}

	public void write(DataOutput out) throws IOException {
		document.write(out);
		out.writeLong(start);
		out.writeLong(length);
	}

	public void readFields(DataInput in) throws IOException {
		Document doc = new Document();
		doc.readFields(in);
		document = doc;
		start = in.readLong();
		length = in.readLong();
		hosts = null;
	}

	@Override
	public String toString() {
		return document.toString() + ":" + start + "+" + length;
	}

}
