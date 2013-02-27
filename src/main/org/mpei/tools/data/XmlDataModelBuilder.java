package org.mpei.tools.data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.ext.PorterStemmer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Builder { @link org.mpei.tools.data.DataModel} from xml.
 * 
 */
public class XmlDataModelBuilder implements DataModelBuilder {
	private static final Logger log = LoggerFactory
			.getLogger(XmlDataModelBuilder.class);
	public static final String TAG_ANNOTATION = "content";
	public static final String TAG_YEAR = "year";
	public static final String TAG_AUTHORS = "authors";
	public static final String TAG_TITLE = "title";

	private final DocumentBuilderFactory dbFactory = DocumentBuilderFactory
			.newInstance();
	private final PorterStemmer stem = new PorterStemmer();

	private DataModel model;
	private AtomicInteger countTokens = new AtomicInteger(0);

	public DataModel build(String path) {
		try {
			File files = getPath(path);
			File[] listFile = files.listFiles();
			String[] labels = files.list();
			model = new DataModel(labels);
			ExecutorService executor = Executors
					.newFixedThreadPool(listFile.length);
			for (File file : listFile) {
				executor.execute(new XmlThread(file));
			}
			executor.shutdown();
			// wait all threads
			while (!executor.isTerminated()) {
			}
			// Document doc = dBuilder.parse(fXmlFile);
			// log.info(countTokens.toString());
		} catch (Exception e) {
			return null;
		}
		return model;
	}

	public DataModel getDataModel() {
		return model;
	}

	public DataModel read(String path) {
		try {
			FileInputStream fstream = new FileInputStream(path);
			DataInputStream in = new DataInputStream(fstream);
			model = new DataModel();
			model.readFields(in);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return model;
	}

	private class XmlThread implements Runnable {

		private File file = null;

		public XmlThread(File file) {
			this.file = file;
		}

		public void run() {
			log.info("Run thread: " + file);
			try {
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(file);
				NodeList nList = doc
						.getElementsByTagName(CoolgaDataParser.TAG_DOCUMENT);

				log.info(String.valueOf(nList.getLength()));
				countTokens.getAndAdd(nList.getLength());
				org.mpei.tools.data.Document[] docs = new org.mpei.tools.data.Document[nList
						.getLength()];
				for (int i = 0; i < nList.getLength(); i++) {
					Node nNode = nList.item(i);
					if (nNode.getNodeType() == Node.ELEMENT_NODE) {
						Element eElement = (Element) nNode;
						// log.info(eElement.getElementsByTagName(TAG_TOKENS).item(0)
						// .getTextContent());
						Node docNode = eElement.getElementsByTagName(
								TAG_ANNOTATION).item(0);
						if (docNode == null) {
							continue;
						}
						org.mpei.tools.data.Document docData = new DefaultDocument();
						docData.setContext(docNode.getTextContent());
						docData.setYear(eElement.getElementsByTagName(TAG_YEAR)
								.item(0).getTextContent());
						docData.setAuthors(eElement
								.getElementsByTagName(TAG_AUTHORS).item(0)
								.getTextContent());
						docData.setName(eElement.getElementsByTagName(TAG_TITLE)
								.item(0).getTextContent());
						docs[i] = docData;
					}
				}
				model.addDocuments(file.getName().replace("[*][.xml]", "$1"),
						docs);
			} catch (Exception e) {
				e.printStackTrace();
			}

			log.info("Complete thread: " + file);
		}
	}

	private File getPath(String path) throws IOException {
		File files = new File(path);
		int i = files.listFiles().length;
		if (i == 0) {
			throw new IOException("In path no files:" + i);
		}
		return files;
	}

	public static void main(String[] args) {
		XmlDataModelBuilder builder = new XmlDataModelBuilder();
		DataOutputStream out = null;
		try {
			DataModel model = builder
					.build("/home/work/git/powerMining/coolga");
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
