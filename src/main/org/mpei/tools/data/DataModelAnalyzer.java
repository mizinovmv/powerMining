package org.mpei.tools.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
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
import com.google.gson.internal.StringMap;

public class DataModelAnalyzer {
	private static final Logger LOG = LoggerFactory
			.getLogger(DataModelAnalyzer.class);
	private static final String REG_EXP = "[a-zA-Zа-яА-я]+";
	private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
	private DataModel model;
	final ArrayList<String> stopWords = new ArrayList<String>();

	public enum Weight {
		TF, TFIDF, TFC,
	}

	public DataModelAnalyzer(DataModel model) {
		this.model = model;
	}

	/**
	 * build model with weight and steming
	 * 
	 * @param type
	 * @param steming
	 * @return
	 */
	public DataModel build(Weight type, Boolean steming, int tokenSize) {
		if (model == null) {
			return null;
		}
		addStopWords(new File("stop/EnglishStopList.txt"));
		addStopWords(new File("stop/RussianStoplist.txt"));
		return calculate(type, steming, tokenSize);
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
				if (streamStem != null) {
					streamStem.close();
				}

			} catch (IOException e2) {
				throw new RuntimeException(e2);
			}
		}
		return token;
	}

	private DataModel calculate(Weight type, Boolean steming, int tokenSize) {
		DataModel tokenModel = new DataModel(model.getLabels());
		DataModelDictionary.MyAnalyzer stemAnalyzer = null;
		if (steming) {
			stemAnalyzer = new DataModelDictionary.MyAnalyzer();
		}
		if (tokenSize == 0) {
			tokenSize = Integer.MAX_VALUE;
		}
		Pattern p = Pattern.compile(REG_EXP);
		Matcher m = null;
		int percent = 0;
		int sizeLabels = model.getLabels().length;
		for (String key : model.getLabels()) {
			Document[] docs = new Document[model.getDocuments(key).size()];
			int i = 0;
			for (Document doc : model.getDocuments(key)) {
				Map<String, Double> tokens = new HashMap<String, Double>();
				TokenStream stream = analyzer.tokenStream(null,
						new StringReader(doc.getContext().toString()));
				int countDocTokens = 0;
				try {
					while ((tokens.size() < tokenSize)
							&& stream.incrementToken()) {
						String token = stream.getAttribute(
								CharTermAttribute.class).toString();

						if (steming && (token.length() != 0)) {
							token = stem(stemAnalyzer, token);
						}
						m = p.matcher(token);
						if (stopWords.contains(token) || !m.matches()) {
							continue;
						}
						Double countI = tokens.get(token);
						double count = (countI == null) ? 0 : countI;
						tokens.put(token, ++count);
						++countDocTokens;
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
				Document tokenDoc = DocumentFabric.newInstance(doc);
				tokenDoc.setContext(tokens);
				docs[i] = tokenDoc;
				++i;
			}
			tokenModel.addDocuments(key, docs);
			++percent;
			LOG.info(String.valueOf(percent * 100 / sizeLabels));
		}
		int percentWeight = 0;
		for (String key : tokenModel.getLabels()) {
			for (Document doc : tokenModel.getDocuments(key)) {
				Similarity sim = new DefaultSimilarity();
				Map<String, Double> tokens = (Map<String, Double>)doc.getContext();
				switch (type) {
				case TF:
					for (Map.Entry<String, Double> entry : tokens.entrySet()) {
						tokens.put(entry.getKey(), entry.getValue()
								/ tokens.size());
					}
					break;
				case TFIDF:
					for (Map.Entry<String, Double> entry : tokens.entrySet()) {
						int df = 0;
						for (String label : tokenModel.getLabels()) {
							for (Document docIdf : tokenModel.getDocuments(label)) {
								if (doc == docIdf) {
									continue;
								}
								Map<String, Double> tmp = (Map<String, Double>)docIdf.getContext();
								if (tmp.containsKey(entry.getKey())) {
									++df;
								}
							}
						}

						int tf = entry.getValue().intValue();
						int numDocs = 0;
						for (String key1 : model.getLabels()) {
							numDocs += model.getDocuments(key1).size();
						}
						tokens.put(entry.getKey(),
								(double) tf * sim.idf(df, numDocs));
					}
					// LOG.info(tokensTfIdf.toString());
					break;
				case TFC:
					double sum = 0;
					for (Map.Entry<String, Double> entry : tokens.entrySet()) {
						int df = 0;
						for (String label : model.getLabels()) {
							for (Document docIdf : model.getDocuments(label)) {
								if (doc == docIdf) {
									continue;
								}
								if (docIdf.getContext().toString()
										.contains(entry.getKey())) {
									++df;
								}
							}
						}

						int tf = entry.getValue().intValue();
						int numDocs = model.getDocuments(key).size();
						double tfidf = (double) tf * sim.idf(df, numDocs);
						tokens.put(entry.getKey(), tfidf);
						sum += (tfidf * tfidf);
					}
					for (Map.Entry<String, Double> entry : tokens.entrySet()) {
						tokens.put(entry.getKey(), entry.getValue() / sum);
					}
					break;
				default:
					break;
				}
				doc.setContext(tokens);
			}
			++percentWeight;
			LOG.info(String.valueOf(percentWeight * 100 / sizeLabels));
		}
		
		return tokenModel;
	}

	private void addStopWords(File file) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				stopWords.add(line.split(" ")[0].trim());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
		Path fs = FileSystems.getDefault().getPath(path);
		try {
			if (!Files.isDirectory(fs)) {
				Files.createDirectory(fs);
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		for (String key : model.getLabels()) {
			try {
				// fr = new FileWriter(new File(path, key.replace(" ","_")));
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

				try {
					if (bw != null) {
						bw.close();
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
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
				while ((line = br.readLine()) != null) {
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

	public static void main(String[] args) {
		String pathModel = "dataModel";
		String pathTokenModel = "tokenDataModel";
		String pathJsonModel = "resources";
		DataOutputStream out = null;
		JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
		DataModel model = builder
				.build("https://classification-mizinov.rhcloud.com/api/",10);
		try {
			FileOutputStream fstream = new FileOutputStream(new File(pathModel));
			out = new DataOutputStream(fstream);
			model.write(out);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				out.close();
			} catch (Exception e2) {
				throw new RuntimeException(e2);
			}
		}
		DataModel model2 = DataModel.read(pathModel);
		DataModelAnalyzer analyzer = new DataModelAnalyzer(model2);
		DataModel tokenModel = analyzer.build(Weight.TFIDF, true, 1);
		try {
			FileOutputStream fstream = new FileOutputStream(new File(
					pathTokenModel));
			out = new DataOutputStream(fstream);
			tokenModel.write(out);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// DataModel tokenModel = DataModel.read(pathTokenModel);
		DataModelAnalyzer.toJSON(tokenModel, pathJsonModel);
	}
}
