package org.mpei.data.document;

import org.apache.hadoop.io.Writable;

public interface Document extends Writable {
	String DEFAULT_VALUE = "__";
	String getName();

	String getClassName();

	String getAuthors();

	String getYear();

	void setName(String name);

	void setClassName(String className);

	void setAuthors(String authors);

	void setYear(String year);

	<T> T getContext();

	<T> void setContext(T context);
}
