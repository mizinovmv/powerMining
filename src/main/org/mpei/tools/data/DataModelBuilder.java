package org.mpei.tools.data;

public interface DataModelBuilder {
	/**
	 * Build DataModel from files.
	 * @param path
	 * @param size
	 *            if size 0,use infinity size
	 * @return
	 */
	DataModel build(String path, int size);
}
