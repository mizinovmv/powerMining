package org.mpei.tools.data;

import org.apache.hadoop.io.Writable;

public interface Document extends Writable {

	String getName();

	String getClassName();

	String getAuthors();

	String getYear();

	void setName(String name);

	void setClassName(String className);

	void setAuthors(String authors);

	void setYear(String year);

	public Writable getContext();

	public <T extends Writable> void setContext(T context);
	
	void clear();
}
