package org.mpei.tools.data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.mpei.data.document.Document;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Dictionary {
	private List<String> tokensAll;

	/*
	 * tokens for every label
	 */
	private Map<String, List<String>> tokensInd;

	public List<String> getAll() {
		return tokensAll;
	}

	public List<String> get(String key) {
		return tokensInd.get(key);
	}

	public void loadTokens(URI cachePath, int size) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				cachePath.toString()));
		try {
			String line = null;
			Document doc = null;
			tokensInd = new HashMap<String, List<String>>();
			tokensAll = new ArrayList<String>();
			List<String> tokens = null;
			JsonParser parser = new JsonParser();
			while ((line = reader.readLine()) != null) {
				JsonElement element = parser.parse(line);
				if (element.isJsonObject()) {
					JsonObject obj = element.getAsJsonObject();
					String className = obj.get("className").getAsString();
					tokens = new ArrayList<String>();
					for (JsonElement elem : obj.get("words").getAsJsonArray()) {
						if (tokens.contains(elem.getAsString())) {
							continue;
						}
						tokens.add(elem.getAsString());
						if (tokensAll.contains(elem.getAsString())) {
							continue;
						}
						tokensAll.add(elem.getAsString());
					}

					tokensInd.put(className, tokens);
				} else {
					throw new RuntimeException();
				}
			}
			if (size > tokensAll.size()) {
				throw new RuntimeException();
			}
			if (size != 0) {
				tokensAll = tokensAll.subList(0, size);
			}
		} finally {
			if (reader != null) {
				reader.close();
			}

		}
	}

}
