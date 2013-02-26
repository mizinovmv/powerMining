package org.mpei.tools.data;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class DocumentArrayWritable extends ArrayWritable{
	
	public DocumentArrayWritable() {
		super(Document.class); 
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		for(Writable value : get()) {
			b.append(value.toString());
			b.append("\n");
		}
		return b.toString();
	}
}
