package org.mpei.knn.kdtree.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mpei.data.document.Document;
import org.mpei.data.document.DocumentFabric;
import org.mpei.knn.kdtree.KnnKdtreeDriver;
import org.mpei.tools.data.Dictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.internal.StringMap;

public class KnnKdtreeBuilder {

	private static final Logger log = LoggerFactory
			.getLogger(KnnKdtreeBuilder.class);
	private static Dictionary dictionary = new Dictionary();

	public static final KDTree buildKDTree(Path input, final Configuration conf) throws IOException  {
		KDTree kdTree = null;
		BufferedReader reader = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			String file = conf.get(KnnKdtreeDriver.TOKEN_CASHE);
			dictionary.loadTokens(new URI(file), 0);
			
			FileStatus[] status = fs.listStatus(input);
			FSDataInputStream fsIn = null;
			
			kdTree =  new KDTree(dictionary.getAll().size());
			for (int i = 0; i < status.length; ++i) {
				fsIn = fs.open(status[i].getPath());
				reader = new BufferedReader(new InputStreamReader(fsIn));
				String line = null;
				Document doc = null;
				while ((line = reader.readLine()) != null) {
					doc = DocumentFabric.fromJson(line);
					double[] coordValues = DocumentFabric.getTokensFreq(doc,
							dictionary);
					kdTree.insert(coordValues,  doc.getClassName());
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (reader != null) {
				reader.close();
			}
			if (fs != null) {
				fs.close();
			}
		}
		return kdTree;
		
		// ArrayList<String> words = null;
		// try {
		// words = readWords(conf);
		// } catch (Exception e) {
		// log.error("Can't read tokenCashe");
		// log.error(e.getMessage());
		// e.printStackTrace();
		// return null;
		// }

		// KDTree kdtree = new KDTree(words.size());
		//
		// try {
		// // String file = conf.get("mapred.input.dir");
		// FileSystem fs = FileSystem.get(conf);
		// FileStatus[] status = fs.listStatus(input);
		// FSDataInputStream fsIn = null;
		// BufferedReader buffReader = null;
		// try {
		// for (int i = 0; i < status.length; ++i) {
		// fsIn = fs.open(status[i].getPath());
		// buffReader = new BufferedReader(new InputStreamReader(fsIn));
		// String line = null;
		// while ((line = buffReader.readLine()) != null) {
		// Document doc = DocumentFabric.fromJson(json.toJSONString());
		// }
		// }
		// } catch (Exception e) {
		// log.error("Can't handle input data");
		// log.error(e.getMessage());
		// e.printStackTrace();
		// } finally {
		// buffReader.close();
		// fs.close();
		// }
		// } catch (Exception e) {
		// log.error("Can't handle input data");
		// log.error(e.getMessage());
		// e.printStackTrace();
		// return null;
		// }
//		return kdTree;
	}

	public static final void writeKDTree(Path output, final Configuration conf,
			KDTree kdTree) throws IOException {
		// String output = conf.get("output");
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = null;
		try {
			out = fs.create(new Path("KDTree.bin"));
			KDTreeWritable.writeKDTree(out, kdTree);
		} finally {
			if (out != null) {
				out.close();
			}

		}

	}

	public static final KDTree readKDTree(final Configuration conf)
			throws IOException {
		// String output = conf.get("output");
		FileSystem fs = FileSystem.get(conf);
		// FSDataInputStream in = fs.open(new Path(output, "KDTree.bin"));
		FSDataInputStream in = fs.open(new Path("KDTree.bin"));
		return KDTreeWritable.readKDTree(in);
	}

//	private static final ArrayList<String> readWords(final Configuration conf)
//			throws IOException, ParseException {
//		ArrayList<String> words = new ArrayList<String>();
//		String file = conf.get(KnnKdtreeDriver.TOKEN_CASHE);
//		FileSystem fs = FileSystem.get(conf);
//		Path filePath = new Path(file);
//		FSDataInputStream fsIn = null;
//		BufferedReader buffReader = null;
//		try {
//			fsIn = fs.open(filePath);
//			buffReader = new BufferedReader(new InputStreamReader(fsIn));
//			int count = 0;
//			String line = null;
//			while ((line = buffReader.readLine()) != null) {
//				JSONObject wordJson = (JSONObject) parser.parse(line);
//				words.add(wordJson.get("word").toString());
//			}
//		} finally {
//			if (buffReader != null) {
//				buffReader.close();
//			}
//		}
//		return words;
//	}
//
//	private static final void insertInKDTree(final JSONObject json,
//			final ArrayList<String> words, KDTree kdTree) {
//		try {
//			Document doc = DocumentFabric.fromJson(json.toJSONString());
//			// String doc = json.get("document").toString();
//			// String className = json.get("class").toString();
//			Object tokens = json.get("tokens");
//			if (tokens == null) {
//				return;
//			}
//			JSONArray coordinates = (JSONArray) parser.parse(tokens.toString());
//			double[] coordValues = new double[words.size()];
//			for (Object tmp : coordinates) {
//				JSONObject coordinate = (JSONObject) tmp;
//				String token = coordinate.get("token").toString();
//				int index = 0;
//				if ((index = words.indexOf(token)) < 0) {
//					log.warn("No index " + token);
//					continue;
//				}
//				coordValues[index] = Double.valueOf(coordinate.get("freq")
//						.toString());
//			}
//			kdTree.insert(coordValues, json);
//		} catch (Exception e) {
//			log.error(e.getMessage());
//			e.printStackTrace();
//			kdTree = null;
//		}
//	}
}
