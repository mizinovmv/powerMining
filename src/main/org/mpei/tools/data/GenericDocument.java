package org.mpei.tools.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class GenericDocument<E extends Writable> implements Document {

	protected static final String DEFAULT_FIELD = "empty";
	protected String name = DEFAULT_FIELD;
	protected String className = DEFAULT_FIELD;
	protected String authors = DEFAULT_FIELD;
	protected String year = DEFAULT_FIELD;
	protected Writable context = new Text(DEFAULT_FIELD);

	public void write(DataOutput out) throws IOException {
		new Text(name).write(out);
		new Text(className).write(out);
		new Text(authors).write(out);
		new Text(year).write(out);
		context.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.name = readAttr(in, Text.class).toString();
		this.className = readAttr(in, Text.class).toString();
		this.authors = readAttr(in, Text.class).toString();
		this.year = readAttr(in, Text.class).toString();
		context = WritableFactories.newInstance(context.getClass());
		context.readFields(in);
	}

	protected <W extends Writable> Writable readAttr(DataInput in,
			Class<W> classType) throws IOException {
		try {
			Writable tmp = WritableFactories.newInstance(classType);
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
		builder.append("context:" + context.toString());
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

	public void setName(String name) {
		this.name = name;
	}

	public void setAuthors(String authors) {
		this.authors = authors;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public <T extends Writable> void setContext(T context) {
		this.context = context;
	}

	public Writable getContext() {
		return context;
	}

	public void clear() {
		this.name = DEFAULT_FIELD;
		this.className = DEFAULT_FIELD;
		this.authors = DEFAULT_FIELD;
		this.year = DEFAULT_FIELD;
		this.context = new Text(DEFAULT_FIELD);
	}

}
