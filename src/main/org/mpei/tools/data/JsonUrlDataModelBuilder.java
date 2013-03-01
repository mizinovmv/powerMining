package org.mpei.tools.data;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.mpei.data.document.Document;
import org.mpei.data.document.DocumentFabric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonUrlDataModelBuilder implements DataModelBuilder {
	private static final Logger LOG = LoggerFactory
			.getLogger(JsonUrlDataModelBuilder.class);
	private static final String TAG_LABEL = "name";
	private static final String TAG_ANNOTATION = "annotation";
	private static final String TAG_LANG = "lang";
	private DataModel model;
	private LANG langEnum;

	enum LANG {
		ENG("en"), RUS("ru");
		String lang;

		LANG(String lang) {
			this.lang = lang;
		}
	}

	public JsonUrlDataModelBuilder() {
		langEnum = LANG.ENG;
	}

	public JsonUrlDataModelBuilder(LANG lang) {
		this.langEnum = lang;
	}

	public DataModel build(String path) {
		try {
			String getClassResult = getUrlResult(path + "getClasses");
			JsonParser parser = new JsonParser();
			JsonElement labelElement = parser.parse(getClassResult);
			JsonArray labelsJson = labelElement.getAsJsonArray();
			// log.info(labelsJson.toString());
			List<String> labels = new ArrayList<String>();
			for (JsonElement label : labelsJson) {
				if (label.isJsonObject()) {
					JsonObject labelObj = (JsonObject) label;
					String getTransResult = getUrlResult("http://translate.yandex.net/api/v1/tr.json/detect?text="
							+ URLEncoder.encode(label.toString(), "UTF-8"));
					JsonObject transElement = (JsonObject)parser.parse(getTransResult);
					if (transElement.get(TAG_LANG).getAsString().equals(langEnum.lang)) {
						labels.add(labelObj.get(TAG_LABEL).getAsString());
						LOG.info(labelObj.get(TAG_LABEL).toString());
					}
				}
			}
			model = new DataModel(labels.toArray(new String[0]));
			for (String label : labels) {
				String getDocumentResult = getUrlResult(path
						+ "getDocuments?class="
						+ URLEncoder.encode(label, "UTF-8"));
				JsonElement attributesElement = parser.parse(getDocumentResult);
				JsonArray attrsJson = attributesElement.getAsJsonArray();
				Document[] docs = new Document[attrsJson.size()];
				int i = 0;
				for (JsonElement attr : attrsJson) {
					if (attr.isJsonObject()) {
						JsonObject attrObj = (JsonObject) attr;
						Document doc = DocumentFabric.newInstance();
						doc.setContext(new Text(attrObj.get(TAG_ANNOTATION).getAsString()));
						doc.setName(attrObj.get(TAG_LABEL).getAsString());
						doc.setClassName(label);
						docs[i] = doc;
//						 LOG.info(doc.toString());
					}
					++i;
				}
				model.addDocuments(label, docs);
				// log.info(attrsJson.toString());
			}
			// log.info(model.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return model;
	}

	private String getUrlResult(String path) throws IOException {
		URL url = new URL(path);
		URLConnection connection = url.openConnection();
		String line;
		StringBuilder builder = new StringBuilder();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				connection.getInputStream()));
		while ((line = reader.readLine()) != null) {
			builder.append(line);
		}
		return builder.toString();
	}

	public static void main(String[] args) {
		JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
		DataOutputStream out = null;
		try {
			DataModel model = builder
					.build("https://classification-mizinov.rhcloud.com/api/");
			FileOutputStream fstream = new FileOutputStream(new File(
					"dataModel"));
			out = new DataOutputStream(fstream);
			model.write(out);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
		} finally {
			try {
				out.close();
			} catch (Exception e2) {
				LOG.error(e2.getMessage());
			}
		}
	}

}
