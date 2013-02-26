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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonUrlDataModelBuilder implements DataModelBuilder {
	private static final Logger log = LoggerFactory
			.getLogger(JsonUrlDataModelBuilder.class);
	private static final String TAG_LABEL = "name";
	private static final String TAG_ANNOTATION = "annotation";

	private DataModel model;

	public DataModel build(String path) {
		try {
			String getClassResult = getUrlResult(path + "getClasses");
			JsonParser parser = new JsonParser();
			JsonElement labelElement = parser.parse(getClassResult);
			JsonArray labelsJson = labelElement.getAsJsonArray();
//			log.info(labelsJson.toString());
			String[] labels = new String[labelsJson.size()];
			int i = 0;
			for (JsonElement label : labelsJson) {
				if (label.isJsonObject()) {
					JsonObject labelObj = (JsonObject) label;
					labels[i] = labelObj.get(TAG_LABEL).toString();
					++i;
				}
			}
			model = new DataModel(labels);
			for (String label : labels) {
				String getDocumentResult = getUrlResult(path
						+ "getDocuments?class="
						+ URLEncoder.encode(label.replace("\"", ""), "UTF-8"));
				JsonElement attributesElement = parser.parse(getDocumentResult);
				JsonArray attrsJson = attributesElement.getAsJsonArray();
				Document[] docs = new Document[attrsJson.size()];
				i = 0;
				for (JsonElement attr : attrsJson) {
					if (attr.isJsonObject()) {
						JsonObject attrObj = (JsonObject) attr;
						Document doc = new Document();
						doc.annotation = attrObj.get(TAG_ANNOTATION).toString();
						doc.name = attrObj.get(TAG_LABEL).toString();
						doc.className = label;
						docs[i] = doc;
						// log.info(attrObj.get(TAG_LABEL).toString());
						// log.info(attrObj.get(TAG_ANNOTATION).toString());
//						log.info(doc.toString());
					}
					++i;
				}
				model.addDocuments(label, docs);
				// log.info(attrsJson.toString());
			}
//			log.info(model.toString());
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
	
	public DataModel read(String path) {
		try {
			FileInputStream fstream = new FileInputStream(path);
			DataInputStream in = new DataInputStream(fstream);
			model = new DataModel();
			model.readFields(in);
//			log.info(model.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return model;
	}
	
	public static void main(String[] args) {
		JsonUrlDataModelBuilder builder = new JsonUrlDataModelBuilder();
		DataOutputStream out = null;
		try {
			DataModel model = builder.build("https://classification-mizinov.rhcloud.com/api/");
			FileOutputStream fstream = new FileOutputStream(new File(
					"dataModel"));
			out = new DataOutputStream(fstream);
			model.write(out);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
		} finally {
			try {
				out.close();
			} catch (Exception e2) {
				log.error(e2.getMessage());
			}
		}
	}


}
