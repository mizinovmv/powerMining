package org.mpei.tools.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.vectorizer.TFIDF;
import org.mpei.data.document.Document;
import org.mpei.data.document.DocumentFabric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

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

	private String stem(DataModelDictionary.MyAnalyzer stemAnalyzer,
			String token) {
		TokenStream streamStem = null;
		try {
			streamStem = stemAnalyzer
					.tokenStream(null, new StringReader(token));
			while (streamStem.incrementToken()) {
				token = streamStem.getAttribute(CharTermAttribute.class)
						.toString();
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				streamStem.close();
			} catch (IOException e2) {
				throw new RuntimeException(e2);
			}
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
				} finally {
					try {
						stream.close();
					} catch (IOException e2) {
						throw new RuntimeException(e2);
					}
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
				Document tokenDoc = DocumentFabric.newInstance();
				tokenDoc.setContext(tokensTfIdf);
				docs[i] = tokenDoc;
				++i;
				if (out != null) {
					try {
						out.writeUTF(DocumentFabric.toJson(tokenDoc));
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

	public static class DocumentAdapter<T extends Document> implements
			JsonSerializer<T>, JsonDeserializer<T> {
		public JsonElement serialize(T src, Type typeOfSrc,
				JsonSerializationContext context) {
			JsonObject result = new JsonObject();
			JsonElement elem = null;
			for (Field field : src.getClass().getDeclaredFields()) {
				try {
					elem = context.serialize(field.get(src));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				result.add(field.getName(), elem);
			}
			return result;
		}

		public T deserialize(JsonElement json, Type typeOfT,
				JsonDeserializationContext context) throws JsonParseException {
			JsonObject jsonObject = json.getAsJsonObject();
			Document doc = DocumentFabric.newInstance();
			Iterator<Entry<String, JsonElement>> it = jsonObject.entrySet()
					.iterator();
			for (Field field : doc.getClass().getDeclaredFields()) {
				try {
					field.set(
							doc,
							context.deserialize(it.next().getValue(),
									field.getType()));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			return (T) doc;
		}
	}

	public static void toJSON(DataModel model, String path) {
		FileWriter fr = null;
		BufferedWriter bw = null;
		for (String key : model.getLabels()) {
			try {
				fr = new FileWriter(new File(path, key));
				bw = new BufferedWriter(fr);
				for (Document doc : model.getDocuments(key)) {
					String line = DocumentFabric.toJson(doc)
							+ System.getProperty("line.separator");
					bw.write(line);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if (bw != null) {
					try {
						bw.close();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}

		}
	}

	public static void fromJSON(DataModel model, String path)
			throws IOException {
		File[] files = new File(path).listFiles();
		ArrayList<Document> docs = new ArrayList<Document>();
		Document[] a = new Document[0];
		FileReader fr = null;
		BufferedReader br = null;
		String line = null;
		for (File key : files) {
			try {
				fr = new FileReader(key);
				br = new BufferedReader(fr);
				while((line = br.readLine()) != null){
					docs.add(DocumentFabric.fromJson(line));
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			model.addDocuments(key.getName(), docs.toArray(a));
		}

	}
}
