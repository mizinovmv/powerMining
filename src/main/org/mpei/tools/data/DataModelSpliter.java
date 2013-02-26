package org.mpei.tools.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataModelSpliter {
	final Logger log = LoggerFactory.getLogger(DataModelSpliter.class);

	private DataModel model;
	private int percent;

	/**
	 * split DataModel to training and test data
	 * 
	 * @param model
	 *            {@link DataModel}
	 * @param percent
	 *            split 0< percent< 100 for training data
	 */
	public DataModelSpliter(DataModel model, int percent) {
		this.model = model;
		this.percent = percent;
	}

	public DataModel getTrainingData() {
		DataModel trainModel = new DataModel(model.getLabels());
		for (String key : model.getLabels()) {
			int size = model.getDocuments(key).size();
			int splitNumber = Math.round(size * (percent / 100f));
			int i = 0;
			Document[] docs = new Document[splitNumber];
			for (Document doc : model.getDocuments(key)) {
				if (i >= splitNumber) {
					break;
				}
				docs[i] = doc;
				++i;
			}
			trainModel.addDocuments(key, docs);
		}
		return trainModel;
	}

	public DataModel getTestData() {
		DataModel testModel = new DataModel(model.getLabels());
		for (String key : model.getLabels()) {
			int size = model.getDocuments(key).size();
			int splitNumber = Math.round(size * (percent / 100f));
			int i = 0;
			Document[] docs = new Document[size - splitNumber];
			for (Document doc : model.getDocuments(key)) {
				if (i >= splitNumber) {
					docs[i] = doc;
				} else
					break;
				++i;
			}
			testModel.addDocuments(key, docs);
		}
		return testModel;
	}

}
