package org.mpei.tools.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public abstract class AbstractDocument implements Document {

	protected static final String DEFAULT_FIELD = "undefined";

	protected String name = DEFAULT_FIELD;
	protected String className = DEFAULT_FIELD;
	protected String authors = DEFAULT_FIELD;
	protected String year = DEFAULT_FIELD;

	public abstract void writeFileds(DataOutput out) throws IOException;

	public void write(DataOutput out) throws IOException {
		new Text(name).write(out);
		new Text(className).write(out);
		new Text(authors).write(out);
		new Text(year).write(out);
		writeFileds(out);
	}

	public abstract void read(DataInput in) throws IOException;

	public void readFields(DataInput in) throws IOException {
		this.name = readAttr(in,Text.class).toString();
		this.className = readAttr(in,Text.class).toString();
		this.authors =readAttr(in,Text.class).toString();
		this.year =readAttr(in,Text.class).toString();
		read(in);
	}

	protected <W extends Writable> W readAttr(DataInput in, Class<W> classType)
			throws IOException {
		try {
			W tmp = classType.newInstance();
			tmp.readFields(in);
			return tmp;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

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
		return builder.toString();
	}

	public String getName() {
		return name;
	}

	public String getClassName() {
		return className;
	}

	public String getAuthors() {
		return authors;
	}

	public String getYear() {
		return year;
	}

	public abstract String getContext();

	public void setName(String name) {
		this.name = name;
	}

	public void setAuthors(String authors) {
		this.authors = authors;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public abstract void setContext(String context);

	public void setClassName(String className) {
		this.className = className;
	}

}
