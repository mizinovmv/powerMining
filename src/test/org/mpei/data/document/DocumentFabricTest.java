package org.mpei.data.document;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DocumentFabricTest extends Assert {
	static String path = "test/DocumentFabricTest";
	static DataOutputStream out = null;
	static DataInputStream in = null;

	@BeforeClass
	public static void setup() {
		try {
			FileOutputStream fstreamOut = new FileOutputStream(new File(path));
			FileInputStream fstreamIn = new FileInputStream(new File(path));
			out = new DataOutputStream(fstreamOut);
			in = new DataInputStream(fstreamIn);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void test() throws IOException {
		Document doc = DocumentFabric.newInstance();
		assertNotNull(doc);
		int i = 1;
		doc.setContext(i);
		assertEquals(doc.getContext(), 1);
		Map<String, Double> map = new HashedMap();
		map.put("word1", 4.3d);
		doc.setContext(map);
		assertEquals(doc.getContext(), map);
		doc.write(out);
		Document doc2 = DocumentFabric.newInstance();
		doc2.readFields(in);
		assertEquals(doc, doc2);
	}

}
