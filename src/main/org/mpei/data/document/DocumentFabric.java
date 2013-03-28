package org.mpei.data.document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.mpei.tools.data.Dictionary;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.internal.StringMap;

/**
 * Document fabric with Gson serialization.
 * 
 */
public class DocumentFabric {
	private static Gson gson;
	static {
		GsonBuilder builder = new GsonBuilder().serializeNulls();
		builder = builder.registerTypeHierarchyAdapter(Writable.class,
				new WritableAdapter<Writable>());
		gson = builder.registerTypeHierarchyAdapter(Document.class,
				new DocumentAdapter<Document>()).create();
	}

	/**
	 * Default constructor {@link Document}.
	 * 
	 * @param doc
	 * @return Copy
	 */
	public static Document newInstance() {
		return new BaseDocument();
	}

	/**
	 * New instance as copy {@link Document}.
	 * 
	 * @param doc
	 * @return new copy of doc
	 */
	public static Document newInstance(Document doc) {
		return new BaseDocument(doc);
	}

	/**
	 * Serialization {@link Document} to json string.
	 * 
	 * @param doc
	 *            {@link Document}
	 * @return json String
	 */
	public static String toJson(Document doc) {
		return gson.toJson(doc);
	}

	/**
	 * Deserialization {@link Document} from json string.
	 * 
	 * @param json
	 *            Json string
	 * @return {@link Document}
	 */
	public static Document fromJson(String json) {
		Document doc = gson.fromJson(json, BaseDocument.class);
		if (doc == null) {
			throw new RuntimeException();
		}
		return doc;
	}

	public static double[] getTokensFreq(Document doc, Dictionary dictionary) {
		StringMap<Double> map = (StringMap<Double>) doc.getContext();
		List<String> dict = dictionary.get(doc.getClassName());
		// List<String> dict = dictionary.getAll();
		double[] docV = new double[dictionary.getAll().size()];
		int index = 0;
		for (Map.Entry<String, Double> entry : map.entrySet()) {
			if ((index = dict.indexOf(entry.getKey())) < 0) {
				continue;
			}
			if (index < dictionary.getAll().size()) {
				docV[index] = entry.getValue();
			}
		}
		return docV;
	}

	public static double[] getAllTokensFreq(Document doc, Dictionary dictionary) {
		StringMap<Double> map = (StringMap<Double>) doc.getContext();
		List<String> dict = dictionary.getAll();
		double[] docV = new double[dictionary.getAll().size()];
		int index = 0;
		for (Map.Entry<String, Double> entry : map.entrySet()) {
			if ((index = dict.indexOf(entry.getKey())) < 0) {
				continue;
			}
			docV[index] = entry.getValue();

		}
		return docV;
	}

	public static double[] getTokensRound(Document doc, Dictionary dictionary,
			int delimiter) {
		StringMap<Double> map = (StringMap<Double>) doc.getContext();
		// List<String> dict = dictionary.get(doc.getClassName());
		List<String> dict = dictionary.getAll();
		double[] docV = new double[dictionary.getAll().size()];
		int index = 0;
		for (Map.Entry<String, Double> entry : map.entrySet()) {
			if ((index = dict.indexOf(entry.getKey())) < 0) {
				continue;
			}
			double r = BigDecimal.valueOf(entry.getValue())
					.setScale(delimiter, BigDecimal.ROUND_DOWN).doubleValue();
			docV[index] = r;
		}
		return docV;
	}

	public static void cacheTrainData(Configuration conf, String train)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(conf.get(train));
		FileStatus[] status = fs.listStatus(hdfsPath);
		for (FileStatus st : status) {
			try {
				DistributedCache.addCacheFile(new URI(st.getPath().toString()),
						conf);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Generic adapter Gson for {@link Document}.
	 * 
	 * @param <T>
	 *            {@link Document}
	 */
	private static class DocumentAdapter<T extends Document> implements
			JsonSerializer<T>, JsonDeserializer<T> {
		public JsonElement serialize(T src, Type typeOfSrc,
				JsonSerializationContext context) {
			JsonObject result = new JsonObject();
			JsonElement elem = null;
			for (Field field : src.getClass().getDeclaredFields()) {
				try {
					elem = context.serialize(field.get(src));
				} catch (IllegalAccessException e) {
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
			JsonElement value = null;
			for (Field field : doc.getClass().getDeclaredFields()) {
				try {
					value = it.next().getValue();
					if (value == null) {
						value = JsonNull.INSTANCE;
					}
					field.set(doc, context.deserialize(value, field.getType()));
				} catch (IllegalAccessException e) {
					throw new RuntimeException(e);
				}
			}
			return (T) doc;
		}
	}

	public static class WritableAdapter<T extends Writable> implements
			JsonSerializer<T>, JsonDeserializer<T> {

		// private static final String CLASSNAME = "c";
		// private static final String INSTANCE = "i";

		public JsonElement serialize(T src, Type typeOfSrc,
				JsonSerializationContext context) {
			return context.serialize(src.toString());
		}

		public T deserialize(JsonElement json, Type typeOfT,
				JsonDeserializationContext context) throws JsonParseException {
			return context.deserialize(json, typeOfT);
		}
	}

	/**
	 * Implements document in electronic form.
	 * 
	 */
	private static class BaseDocument implements Document {

		String name;
		String className;
		String authors;
		String year;
		Object context;

		/**
		 * Default constructor.
		 */
		public BaseDocument() {
		}

		/**
		 * Copy constructor.
		 * 
		 * @param doc
		 *            other {@link Document}
		 */
		public BaseDocument(Document doc) {
			name = doc.getName();
			className = doc.getClassName();
			authors = doc.getAuthors();
			year = doc.getYear();
			context = doc.getContext();
		}

		/**
		 * Write to output.
		 */
		public void write(DataOutput out) throws IOException {
			Text tmp = new Text(toJson(this));
			tmp.write(out);
		}

		/**
		 * Read from input.
		 */
		public void readFields(DataInput in) throws IOException {
			Document doc = fromJson(Text.readString(in));
			this.name = doc.getName();
			this.authors = doc.getAuthors();
			this.year = doc.getYear();
			this.className = doc.getClassName();
			this.context = doc.getContext();
		}

		/**
		 * Get name.
		 */
		public String getName() {
			return name;
		}

		/**
		 * Get class label.
		 */
		public String getClassName() {
			return className;
		}

		/**
		 * Get authors.
		 */
		public String getAuthors() {
			return authors;
		}

		/**
		 * Get publication year.
		 */
		public String getYear() {
			return year;
		}

		/**
		 * Set name.
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * Set authors.
		 */
		public void setAuthors(String authors) {
			this.authors = authors;
		}

		/**
		 * Set publication year.
		 */
		public void setYear(String year) {
			this.year = year;
		}

		/**
		 * Set class label.
		 */
		public void setClassName(String className) {
			this.className = className;
		}

		/**
		 * Get context.
		 */
		public Object getContext() {
			return this.context;
		}

		/**
		 * Set context.
		 */
		public void setContext(Object context) {
			this.context = context;
		}
	}
}
