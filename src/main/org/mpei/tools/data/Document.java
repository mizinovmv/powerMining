package org.mpei.tools.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Document implements Writable {

	public Document() {
	}

	public Document(String name, String className, String authors, String year,
			String annotation) {
		this.name = name;
		this.className = className;
		this.authors = authors;
		this.year = year;
		this.annotation = annotation;
	}

	String name = "undefined";
	String className = "undefined";
	String authors = "undefined";
	String year = "undefined";
	String annotation = "undefined";

	public void write(DataOutput out) throws IOException {
		new Text(name).write(out);
		new Text(className).write(out);
		new Text(authors).write(out);
		new Text(year).write(out);
		new Text(annotation).write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.name = readAttr(in);
		this.className = readAttr(in);
		this.authors = readAttr(in);
		this.year = readAttr(in);
		this.annotation = readAttr(in);
	}

	private String readAttr(DataInput in) throws IOException {
		Text tmp = new Text();
		tmp.readFields(in);
		return tmp.toString();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("name:" + name);
		builder.append("\n");
		builder.append("className:" + className);
		builder.append("\n");
		builder.append("authors:" + authors);
		builder.append("\n");
		builder.append("year:" + year);
		builder.append("\n");
		builder.append("annotation:" + annotation);
		builder.append("\n");
		return builder.toString();
	}
}
