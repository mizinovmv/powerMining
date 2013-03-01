package org.mpei.data.document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class DocumentFabric {
	public static Gson gson;
	static {
		GsonBuilder builder = new GsonBuilder().serializeNulls();
		gson = builder.registerTypeHierarchyAdapter(Document.class,
				new DocumentAdapter<Document>()).create();
	}

	public static Document newInstance() {
		return new BaseDocument();
	}

	public static String toJson(Document doc) {
		return gson.toJson(doc);
	}

	public static Document fromJson(String json) {
		return gson.fromJson(json, BaseDocument.class);
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
			JsonElement value = null;
			for (Field field : doc.getClass().getDeclaredFields()) {
				try {
					value = it.next().getValue();
					if (value == null) {
						value = JsonNull.INSTANCE;
					}
					field.set(doc, context.deserialize(value, field.getType()));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			return (T) doc;
		}
	}

	static class BaseDocument implements Document {

		String name;
		String className;
		String authors;
		String year;
		Object context;

		public void write(DataOutput out) throws IOException {
			Text tmp = new Text(toJson(this));
			tmp.write(out);
		}

		public void readFields(DataInput in) throws IOException {
			Document doc = fromJson(Text.readString(in));
			this.name = doc.getName();
			this.authors = doc.getAuthors();
			this.year = doc.getYear();
			this.className = doc.getClassName();
			this.context = doc.getContext();
		}

		public String getName() {
			return name;
		}

		public String getClassName() {
			return className;
		}

		public String getAuthors() {
			return authors;
		}

		public String getYear() {
			return year;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setAuthors(String authors) {
			this.authors = authors;
		}

		public void setYear(String year) {
			this.year = year;
		}

		public void setClassName(String className) {
			this.className = className;
		}

		public Object getContext() {
			return this.context;
		}

		public <T> void setContext(T context) {
			this.context = (Object) context;
		}
	}
}
