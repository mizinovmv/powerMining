package org.mpei.tools.data;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class DataModelSpliterTest {
	
	DataModelSpliter spliter;
	int percent = 80;
	int fullSize; 
	@Before
	public void setUp() throws Exception {
		String path = "dataModel";
		DataModel model = DataModel.read(path);
		for(String key : model.getLabels()) {
			fullSize += model.getDocuments(key).size();
		}
		spliter = new DataModelSpliter(model, percent);
	}

	@Test
	public void getTrainingDataTest() {
		DataModel trainModel =  spliter.getTrainingData();
		int size1 = 0;
		for(String key : trainModel.getLabels()) {
			size1 += trainModel.getDocuments(key).size();
		}
		assertEquals(percent, Math.round((size1*100f)/(fullSize)));
		DataModel testModel =  spliter.getTestData();
		int size2 = 0;
		for(String key : testModel.getLabels()) {
			size2 += testModel.getDocuments(key).size();
		}
		assertEquals((100-percent), Math.round((size2*100f)/(fullSize)));
		assertEquals(size1 + size2, fullSize);
	}

}
