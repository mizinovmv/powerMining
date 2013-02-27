package org.mpei.tools.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class DefaultDocument extends AbstractDocument {
	protected String annotation = DEFAULT_FIELD;

	@Override
	public String getContext() {
		return annotation;
	}

	@Override
	public void setContext(String context) {
		this.annotation = context;
	}

	@Override
	public String toString() {
		return super.toString() + "annotation:" + annotation + "\n";
	}

	@Override
	public void writeFileds(DataOutput out) throws IOException {
		new Text(annotation).write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.annotation = readAttr(in, Text.class).toString();
	}
}
