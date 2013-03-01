package org.mpei.tools.data;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;

import weka.core.stemmers.SnowballStemmer;

public class DataModelAnalyzer {
	private static final Logger LOG = LoggerFactory
			.getLogger(DataModelAnalyzer.class);

	static {
		GsonBuilder builder = new GsonBuilder();
		builder = builder.registerTypeHierarchyAdapter(Writable.class, new WritableAdapter<Writable>());
		gson = builder.registerTypeHierarchyAdapter(Document.class,
				new DocumentTypeConverter<Document>()).create();
	}

	public static Gson gson;
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
				Document tokenDoc = new GenericDocument();
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
	public class LowercaseEnumTypeAdapterFactory implements TypeAdapterFactory {
		public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
			Class<T> rawType = (Class<T>) type.getRawType();
			if(rawType.equals(Document.class)){
				return new DocumentTypeConverter();
			}
			return null;
		}
//	     public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
//	       Class<T> rawType = (Class<T>) type.getRawType();
//	       if (!rawType.isEnum()) {
//	         return null;
//	       }
//
//	       final Map<String, T> lowercaseToConstant = new HashMap<String, T>();
//	       for (T constant : rawType.getEnumConstants()) {
//	         lowercaseToConstant.put(toLowercase(constant), constant);
//	       }
//
//	       return new TypeAdapter<T>() {
//	         public void write(JsonWriter out, T value) throws IOException {
//	           if (value == null) {
//	             out.nullValue();
//	           } else {
//	             out.value(toLowercase(value));
//	           }
//	         }
//
//	         public T read(JsonReader reader) throws IOException {
//	           if (reader.peek() == JsonToken.NULL) {
//	             reader.nextNull();
//	             return null;
//	           } else {
//	             return lowercaseToConstant.get(reader.nextString());
//	           }
//	         }
//	       };
	     }
	public static class WritableAdapter<T extends Writable> implements JsonSerializer<T>,
			JsonDeserializer<T> {

		private static final String CLASSNAME = "c";
		private static final String INSTANCE = "i";

		public JsonElement serialize(T src, Type typeOfSrc,
				JsonSerializationContext context) {
			JsonObject retValue = new JsonObject();
			String className = src.getClass().getCanonicalName();
			retValue.addProperty(CLASSNAME, className);
			JsonElement elem = context.serialize(src);
			retValue.add(INSTANCE, elem);
			return retValue;
		}

		public T deserialize(JsonElement json, Type typeOfT,
				JsonDeserializationContext context) throws JsonParseException {
			JsonObject jsonObject = json.getAsJsonObject();
			JsonPrimitive prim = (JsonPrimitive) jsonObject.get(CLASSNAME);
			String className = prim.getAsString();

			Class<?> klass = null;
			try {
				klass = Class.forName(className);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new JsonParseException(e.getMessage());
			}
			return context.deserialize(jsonObject.get(INSTANCE), klass);
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
			Document doc = new GenericDocument();
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
		for (String key : model.getLabels()) {
			try {
				FileOutputStream fstream = new FileOutputStream(new File(path
						+ "/" + key));
				DataOutputStream out = new DataOutputStream(fstream);
				out.writeInt(model.getDocuments(key).size());
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

	public static void fromJSON(DataModel model, String path)
			throws IOException {

		DataInputStream in = null;
		FileInputStream fstream = null;
		File[] files = new File(path).listFiles();
		Document[] docs = null;
		for (File key : files) {
			try {
				fstream = new FileInputStream(key);
				in = new DataInputStream(fstream);
				Document doc = null;
				int sizeDoc = in.readInt();
				docs = new Document[files.length];
				for (int i = 0; i < sizeDoc; ++i) {
					docs[i] = gson
							.fromJson(in.readUTF(), GenericDocument.class);
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} finally {
				in.close();
			}
			model.addDocuments(key.getName(), docs);
		}

	}
}
