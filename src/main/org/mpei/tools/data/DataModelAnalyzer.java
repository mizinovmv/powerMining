package org.mpei.tools.data;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.vectorizer.TFIDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataModelAnalyzer {
	private static final Logger LOG = LoggerFactory
			.getLogger(DataModelAnalyzer.class);
	private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);

	private DataModel model;

	enum Weight {
		TFIDF
	}

	public DataModelAnalyzer(DataModel model) {
		this.model = model;
	}

	private DataModel tfIdf() {
		DataModel tokenModel = new DataModel(model.getLabels());
		for (String key : model.getLabels()) {
			Document[] docs = new Document[model.getDocuments(key).size()];
			int i = 0;
			for (Document doc : model.getDocuments(key)) {
				Map<String, Integer> tokens = new HashMap<String, Integer>();
				MapWritable tokensTfIdf = new MapWritable();
				TokenStream stream = analyzer.tokenStream(null,
						new StringReader(doc.getContext()));
				try {
					while (stream.incrementToken()) {
						String token = stream.getAttribute(
								CharTermAttribute.class).toString();
						Integer countI = tokens.get(token);
						int count = (countI == null) ? 0 : countI;
						tokens.put(token, ++count);
					}
				} catch (IOException e) {
					e.printStackTrace();
					LOG.error(e.getMessage());
				}
				// LOG.info(tokens.toString());
				for (Map.Entry<String, Integer> entry : tokens.entrySet()) {
					int df = 0;
					TFIDF tfIdf = new TFIDF();
					for (Document docIdf : model.getDocuments(key)) {
						if (doc == docIdf) {
							continue;
						}
						if (docIdf.getContext().contains(entry.getKey())) {
							++df;
						}
					}
					int tf = entry.getValue();
					int numDocs = model.getDocuments(key).size();
					tokensTfIdf.put(
							new Text(entry.getKey()),
							new DoubleWritable(tfIdf.calculate(tf, df, 0,
									numDocs)));
				}
				// LOG.info(tokensTfIdf.toString());
				TokenDocument<MapWritable> tokenDoc = new TokenDocument<MapWritable>();
				tokenDoc.setContext(tokensTfIdf);
				docs[i] = tokenDoc;
				++i;
			}
			tokenModel.addDocuments(key, docs);
		}
		return tokenModel;
	}

	public DataModel build(Weight type) {
		DataModel tokenModel = null;
		switch (type) {
		case TFIDF:
			tokenModel = tfIdf();
			break;
		default:
			break;
		}

		return tokenModel;
	}

	public static void main(String[] args) {
		String pathModel = "dataModel";
		String pathTokenModel = "tokenDataModel";
		// JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
		// DataModel model = builder
		// .build("https://classification-mizinov.rhcloud.com/api/");
		//
		// try {
		// FileOutputStream fstream = new FileOutputStream(new File(path));
		// DataOutputStream out = new DataOutputStream(fstream);
		// model.write(out);
		// } catch (Exception e) {
		// e.printStackTrace();
		// throw new RuntimeException(e);
		// }
		DataModel model = DataModel.read(pathModel);
		DataModelAnalyzer analyzer = new DataModelAnalyzer(model);
		DataModel tokenModel = analyzer.build(Weight.TFIDF);
		try {
			FileOutputStream fstream = new FileOutputStream(new File(pathTokenModel));
			DataOutputStream out = new DataOutputStream(fstream);
			tokenModel.write(out);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}		
	}
}
