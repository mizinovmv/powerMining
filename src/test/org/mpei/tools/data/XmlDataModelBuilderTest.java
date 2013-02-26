package org.mpei.tools.data;

import static org.junit.Assert.*;

import org.junit.Test;

public class XmlDataModelBuilderTest {

	@Test
	public void buildDataModelTest() {
		XmlDataModelBuilder builder = new XmlDataModelBuilder();
		String path = "/home/work/git/powerMining/coolga"; 
		DataModel model1 = builder.build(path);
		DataModel model2 = builder.read("dataModel");
		assertEquals(model1, model2);
	}
	
	

}
