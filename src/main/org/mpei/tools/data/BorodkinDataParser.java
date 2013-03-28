package org.mpei.tools.data;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.stream.XMLStreamReader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sun.org.apache.xerces.internal.parsers.DOMParser;

public class BorodkinDataParser {
	public static final String PATH = "/home/dom/Dropbox/magistr/EnglishData/RI";
	public static final DocumentBuilderFactory docFactory = DocumentBuilderFactory
			.newInstance();
	public static final String TAG_CLASS_NAMES = "ClNames:";
	public static final String TAG_CLASS = "CLASS:";
	public static final String TAG_TITTLE = "Title:";
	public static final String TAG_ABSTRACT = "Abstract:";

	public static final String REGEX_TITTLE = "(" + TAG_TITTLE + ")(.+)";
	public static final String REGEX_ABSTRACT = "(" + TAG_ABSTRACT + ")(.+)";
	public static final String REGEX_WORD = "\\w+";

	public static final String TAG_ROOT = "documents";
	public static final String TAG_DOCUMENT = "document";

	private static DocumentBuilder docBuilder;
	private final String[] tags = new String[] { "year", "authors", "title",
			"content" };

	public void parse() {
		File directory = new File(BorodkinDataParser.PATH);
		File[] fileNames = directory.listFiles();
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
			BorodkinDataParser.docBuilder = docFactory.newDocumentBuilder();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

		for (File fileName : fileNames) {
			System.out.println(fileName.getName());
			DataInputStream in = null;
			try {
				FileInputStream fstream = new FileInputStream(fileName);
				in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine = null;
				String[] splitString = null;
				String[] classNames = null;
				StreamResult[] results = null;
				int num = 0;
				Document[] documents = null;
				while ((strLine = br.readLine()) != null) {
					if (strLine.contains(TAG_CLASS_NAMES)) {
						classNames = strLine.replace(TAG_CLASS_NAMES, "")
								.split(";");
						int i = 0;
						results = new StreamResult[classNames.length];
						documents = new Document[classNames.length];
						for (String name : classNames) {
							if(documents[i] != null) {
								++i;
								continue;
							}
							String nameFile = "resources/borodkin/"
									+ name.trim().replace(" ", "_") + ".xml";
							File fileXml = new File(nameFile);
							if(fileXml.exists()) {
								documents[i] = docBuilder.parse(fileXml);
							} else {
								documents[i] = docBuilder.newDocument();
								Element rootElement = documents[i]
										.createElement(TAG_ROOT);
								documents[i].appendChild(rootElement);
							}
							FileWriter writer = new FileWriter(nameFile);
							results[i] = new StreamResult(writer);
							++i;
						}

					} else if (strLine.contains(TAG_CLASS)) {
						splitString = new String[4];
						num = Integer.valueOf(strLine.replace(TAG_CLASS, "")
								.trim()) - 1;
						continue;
					} else if (strLine.contains(TAG_TITTLE)) {
						splitString[2] = strLine.replace(TAG_TITTLE, "");
					} else if (strLine.contains(TAG_ABSTRACT)) {
						strLine = strLine.replace(TAG_ABSTRACT, "");
						List<String> words = Arrays.asList(strLine
								.split("[ ;.]"));
						StringBuilder strBuilder = new StringBuilder();
						Pattern pattern = Pattern.compile(REGEX_WORD);
						for (String word : words) {
							Matcher m = pattern.matcher(word);
							if (m.matches()) {
								strBuilder.append(word);
								strBuilder.append(" ");
							}
						}
						splitString[3] = strBuilder.toString();
						List<String> list = new ArrayList<String>(
								Arrays.asList(splitString));
						Element docElement = documents[num]
								.createElement(TAG_DOCUMENT);
						Element doc = documents[num].getDocumentElement();
						doc.appendChild(docElement);
						// documents[num].createTextNode("/n");
						int i = 0;
						for (String tag : tags) {
							// System.out.println(splitString[i]);
							String line = list.get(i);
							if (line == null) {
								line = "";
							}
							Element element = documents[num].createElement(tag);
							docElement.appendChild(element);

							element.appendChild(documents[num]
									.createTextNode(line));
							// element.appendChild(documents[num]
							// .createTextNode("/n"));
							++i;
						}
					}
				}
				for (int i = 0; i < documents.length; ++i) {
					TransformerFactory transformerFactory = TransformerFactory
							.newInstance();
					Transformer transformer = transformerFactory
							.newTransformer();
					DOMSource source = new DOMSource(documents[i]);
					transformer.transform(source, results[i]);
				}
			} catch (Exception e) {// Catch exception if any
				e.printStackTrace();
				System.err.println("Error: " + e.getMessage());
			} finally {
				try {
					in.close();
				} catch (IOException e) {
					throw new RuntimeException("can't close inputStream");
				}
			}
			System.out.println("File saved!");
		}
	}

	public static void main(String[] args) {
		try {
			BorodkinDataParser parser = new BorodkinDataParser();
			parser.parse();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		// try {
		// XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		// XMLStreamReader reader = inputFactory
		// .createXMLStreamReader(new FileReader(
		// "/home/work/git/powerMining/resources/borodkin/Agents_Multi-agentSystems.xml"));
		// while (reader.hasNext()) {
		// reader.next();
		// }
		// } catch (Exception e) {
		// throw new RuntimeException(e);
		// }

	}
}
