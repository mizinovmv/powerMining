package org.mpei.json;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonLoadWriter {
	
	private static final Logger log = LoggerFactory.getLogger(JsonLoadWriter.class);
	private static JSONParser parser;
	
	public static void httpGet(FileSystem fs,String urlStr, String fileName)
			throws IOException {
		URL url = new URL(urlStr);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		if (conn.getResponseCode() != 200) {
			throw new IOException(conn.getResponseMessage());
		}

		// Buffer the result into a string
		BufferedReader rd = new BufferedReader(new InputStreamReader(
				conn.getInputStream()));
		try {
			// Create file
//			FileWriter fstream = new FileWriter(fileName);
			String path = fs.getConf().get("datareader.inpath") + fileName;
			FSDataOutputStream out = fs.create(new Path(path));
//			BufferedWriter out = new BufferedWriter(fstream);
			parser = new JSONParser();
			JSONArray classes = (JSONArray) parser.parse(rd.readLine());
			for (Object tmp : classes) {
				JSONObject class_ = (JSONObject) tmp;
				out.write(class_.toString().getBytes());
				out.writeBytes("\n");
			}
			out.close();
		} catch (Exception e) {// Catch exception if any
			log.error("Error: ", e);
		} finally {
			rd.close();
			// Close the output stream
			conn.disconnect();
		}

	}
}
