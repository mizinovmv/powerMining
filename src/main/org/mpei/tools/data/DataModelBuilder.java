package org.mpei.tools.data;

public interface DataModelBuilder {
	
	DataModel build(String path);
	
	DataModel read(String path);
}
