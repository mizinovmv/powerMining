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
	
	public String getContext();
	
	public void setContext(String context);
}
