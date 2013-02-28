package org.mpei.tools.data;

import java.io.DataOutput;
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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Version;
import org.apache.mahout.vectorizer.TFIDF;
import org.json.simple.JSONObject;
import org.mpei.tools.data.DataModelDictionary.MyAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import weka.core.stemmers.SnowballStemmer;

public class DataModelAnalyzer {
	private static final Logger LOG = LoggerFactory
			.getLogger(DataModelAnalyzer.class);
	private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
	private final Gson gson = new Gson();
	private DataModel model;

	enum Weight {
		TFIDF
	}

	public DataModelAnalyzer(DataModel model) {
		this.model = model;
	}

	private String stem(DataModelDictionary.MyAnalyzer stemAnalyzer,
			String token) {
		try {
			TokenStream streamStem = stemAnalyzer.tokenStream(null,
					new StringReader(token));
			while (streamStem.incrementToken()) {
				token = streamStem.getAttribute(CharTermAttribute.class)
						.toString();
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return token;
	}

	private DataModel tfIdf(Boolean steming, DataOutput out) {
		DataModel tokenModel = new DataModel(model.getLabels());
		DataModelDictionary.MyAnalyzer stemAnalyzer = null;
		if (steming) {
			stemAnalyzer = new DataModelDictionary.MyAnalyzer();
		}
		int percent = 0;
		int sizeLabels = model.getLabels().length;
		for (String key : model.getLabels()) {
			Document[] docs = new Document[model.getDocuments(key).size()];
			int i = 0;
			for (Document doc : model.getDocuments(key)) {
				Map<String, Integer> tokens = new HashMap<String, Integer>();
				MapWritable tokensTfIdf = new MapWritable();
				// LOG.info(doc.getContext().toString());
				TokenStream stream = analyzer.tokenStream(null,
						new StringReader(doc.getContext().toString()));
				try {
					while (stream.incrementToken()) {
						String token = stream.getAttribute(
								CharTermAttribute.class).toString();
						if (steming && token.length() != 0) {
							token = stem(stemAnalyzer, token);
						}
						Integer countI = tokens.get(token);
						int count = (countI == null) ? 0 : countI;
						tokens.put(token, ++count);
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				// LOG.info(tokens.toString());
				for (Map.Entry<String, Integer> entry : tokens.entrySet()) {
					int df = 0;
					TFIDF tfIdf = new TFIDF();
					for (Document docIdf : model.getDocuments(key)) {
						if (doc == docIdf) {
							continue;
						}
						if (docIdf.getContext().toString()
								.contains(entry.getKey())) {
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
				Document tokenDoc = new GenericDocument<MapWritable>();
				tokenDoc.setContext(tokensTfIdf);
				docs[i] = tokenDoc;
				++i;
				if (out != null) {
					try {
						out.writeUTF(gson.toJson(doc));
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			tokenModel.addDocuments(key, docs);
			++percent;
			LOG.info(String.valueOf(percent * 100 / sizeLabels));
		}
		return tokenModel;
	}

	public DataModel build(Weight type, Boolean steming, DataOutput out) {
		if (model == null) {
			return null;
		}
		DataModel tokenModel = null;
		switch (type) {
		case TFIDF:
			tokenModel = tfIdf(steming, out);
			break;
		default:
			break;
		}

		return tokenModel;
	}

	public static void toJSON(DataModel model, String path) {
		Gson gson = new Gson();
		for (String key : model.getLabels()) {
			try {
				FileOutputStream fstream = new FileOutputStream(new File(key));
				DataOutputStream out = new DataOutputStream(fstream);
				for (Document doc : model.getDocuments(key)) {
					out.writeUTF(gson.toJson(doc));
				}
				out.close();
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

	public static void main(String[] args) {
		String pathModel = "dataModel";
		String pathTokenModel = "tokenDataModel";
		JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
		DataModel model = builder
				.build("https://classification-mizinov.rhcloud.com/api/");
		DataOutputStream out = null;
		try {
			FileOutputStream fstream = new FileOutputStream(new File(pathModel));
			out = new DataOutputStream(fstream);
//			model.write(out);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// DataModel model = DataModel.read(pathModel);
		DataModelAnalyzer analyzer = new DataModelAnalyzer(model);
		DataModel tokenModel = analyzer.build(Weight.TFIDF, true, out);
		try {
			FileOutputStream fstream = new FileOutputStream(new File(
					pathTokenModel));
			out = new DataOutputStream(fstream);
			tokenModel.write(out);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// DataModel tokenModel = DataModel.read(pathTokenModel);
		DataModelAnalyzer.toJSON(tokenModel, "input");
	}
}
