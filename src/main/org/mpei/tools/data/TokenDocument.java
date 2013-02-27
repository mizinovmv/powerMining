package org.mpei.tools.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;
import com.thoughtworks.xstream.mapper.MapperWrapper;

/**
 * Document with tokens and there frequency
 * 
 * @author work
 * 
 */
public class TokenDocument<T extends Writable> extends AbstractDocument {
	protected T tokens;

	public TokenDocument() {
	}

	@Override
	public String getContext() {
		return deparse(tokens);
	}

	@Override
	public void setContext(String context) {
		this.tokens = parse(context);
	}
	
	public void setContext(T context) {
		this.tokens = context;
	}
	
	
	@Override
	public void writeFileds(DataOutput out) throws IOException {
		tokens.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		Class<T> c = null;
		this.tokens = readAttr(in, c);
	}

	protected T parse(String context) {
		return null;
	}

	protected String deparse(T context) {
		return new String();
	}
}
