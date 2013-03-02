package org.mpei.data.document;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DocumentFabricTest extends Assert {
	static String path = "test/DocumentFabricTest";

	static BufferedWriter writer;
	static BufferedReader reader;

	@Test
	public void setup() {
		FileWriter out = null;
		FileReader in = null;
		try {
			out = new FileWriter(new File(path));
			writer = new BufferedWriter(out);
			test();
//			in = new FileReader(new File(path));
//			reader = new BufferedReader(in);
//			Document doc2 = DocumentFabric.fromJson(reader.readLine());
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void test() throws IOException {
		Document doc = DocumentFabric.newInstance();
		assertNotNull(doc);
		int h = 1;
		doc.setContext(h);
		assertEquals(doc.getContext(), 1);
		Map<String, Double> map = new HashedMap();
		for (int i = 0; i < 10; ++i) {
			map.put("word" + i, Double.valueOf(i));
		}

		doc.setContext(map);
		assertEquals(doc.getContext(), map);
		String tmp = DocumentFabric.toJson(doc);
		for (int i = 0; i < 10; ++i) {
			writer.write(tmp);
			writer.flush();
		}
		
	}

}
