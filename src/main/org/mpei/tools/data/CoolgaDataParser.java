package org.mpei.tools.data;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sun.org.apache.xerces.internal.parsers.DOMParser;

public class CoolgaDataParser {
	public static final String PATH = "/home/work/Dropbox/magistr/Data";
	public static final String IGNORE_NAME = "Description.txt";
	public static final DocumentBuilderFactory docFactory = DocumentBuilderFactory
			.newInstance();
	public static final String TAG_ROOT = "documents";
	public static final String TAG_DOCUMENT = "document";
	private static DocumentBuilder docBuilder;
	private final String[] tags = new String[] { "year", "authors", "title",
			"content" };
	private StreamResult result;

	private void xmlWrite(String className, List<String> list) {
		String tmp = list.get(2);
		if (tmp.length() == 0 && tmp.equals("Without Abstract")) {
			return;
		}
		// int j = 0;
		// for (String attr : list) {
		// System.out.println(String.valueOf(j) + " "+attr);
		// j++;
		// }
		Document document = docBuilder.newDocument();
		Element rootElement = document.createElement(TAG_ROOT);
		document.appendChild(rootElement);
		Element docElement = document.createElement(TAG_DOCUMENT);
		rootElement.appendChild(docElement);
		int i = 0;
		for (String tag : tags) {
			// System.out.println(splitString[i]);
			String line = list.get(i);
			Element element = document.createElement(tag);
			docElement.appendChild(element);
			String pattern = "(^.*;;)(.*)";
			// System.out.println(line.replaceAll(pattern, "$2"));
			element.appendChild(document.createTextNode(line.replaceAll(
					pattern, "$2")));
			++i;
		}

	}

	private void toFile() throws Exception {
		// write the content into xml file
		TransformerFactory transformerFactory = TransformerFactory
				.newInstance();
		Transformer transformer = null;
		try {
			transformer = transformerFactory.newTransformer();
//			DOMSource source = new DOMSource(document);
			// Output to console for testing
			// StreamResult result = new StreamResult(System.out);
//			transformer.transform(source, result);
		} catch (Exception e) {
			throw e;
		}
	}

	public void parse() {
		File directory = new File(CoolgaDataParser.PATH);
		File[] classNames = directory.listFiles();
		DOMParser domParser = new DOMParser();

		StringBuilder builder = new StringBuilder();
		String tagString = null;
		builder.append(tags[0]);
		for (int i = 1; i < tags.length; ++i) {
			builder.append("|");
			builder.append(tags[i]);
		}
		tagString = builder.toString();
		try {
			CoolgaDataParser.docBuilder = docFactory.newDocumentBuilder();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

		for (File className : classNames) {
			File[] years = className.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					if (name.equals(CoolgaDataParser.IGNORE_NAME))
						return false;
					else
						return true;
				}
			});
			System.out.println(className.getName());

			for (File year : years) {
				System.out.println(year);
				DataInputStream in = null;
				try {
					FileInputStream fstream = new FileInputStream(year);
					in = new DataInputStream(fstream);
					BufferedReader br = new BufferedReader(
							new InputStreamReader(in));
					result = new StreamResult(new File("coolga/"
							+ className.getName().replace(" ", "_") + ".xml"));

					String strLine = null;
					while ((strLine = br.readLine()) != null) {
						// System.out.println(strLine);

						String[] splitString = strLine.split("</?(" + tagString
								+ ")>+");
						List<String> list = new ArrayList<String>(
								Arrays.asList(splitString));
						list.removeAll(Arrays.asList("", null));

						xmlWrite(className.getName(), list);

					}
					
					toFile();
				} catch (Exception e) {// Catch exception if any
					System.err.println("Error: " + e.getMessage());
				} finally {
					try {
						in.close();
					} catch (IOException e) {
						throw new RuntimeException("can't close inputStream");
					}
				}
			}

			System.out.println("File saved!");
		}
	}

	public static void main(String[] args) {
		try {
			CoolgaDataParser parser = new CoolgaDataParser();
			parser.parse();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}
}
