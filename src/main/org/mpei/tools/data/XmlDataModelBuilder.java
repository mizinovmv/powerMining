package org.mpei.tools.data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.Text;
import org.jfree.util.Log;
import org.mpei.data.document.DocumentFabric;
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
	private static final Logger LOG = LoggerFactory
			.getLogger(XmlDataModelBuilder.class);
	public static final String TAG_ANNOTATION = "content";
	public static final String TAG_YEAR = "year";
	public static final String TAG_AUTHORS = "authors";
	public static final String TAG_TITLE = "title";
	public static final String XML_REGEX = "(.*)(\\.xml)$";
	private final DocumentBuilderFactory dbFactory = DocumentBuilderFactory
			.newInstance();
	private final PorterStemmer stem = new PorterStemmer();

	private DataModel model;
	private AtomicInteger countTokens = new AtomicInteger(0);

	/**
	 * build {@link DataModel} from xml documents
	 * 
	 * @param path
	 *            directory with xml files
	 */
	public DataModel build(String path) {
		try {
			File files = getPath(path);
			File[] listFile = files.listFiles();
			String[] labels = new String[files.list().length];
			int i = 0;

			Pattern pattern = Pattern.compile(XML_REGEX);
			for (String nameLabel : files.list()) {
				Matcher m = pattern.matcher(nameLabel);
				if (m.matches()) {
					labels[i] = m.group(1);
					++i;
				}

			}
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
			e.printStackTrace();
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
			LOG.info("Run thread: " + file);
			try {
				Pattern pattern = Pattern.compile(XML_REGEX);
				Matcher m = pattern.matcher(file.getName());
				String className = null;
				if (m.matches()) {
					className = m.group(1);
				}
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(file);
				NodeList nList = doc
						.getElementsByTagName(CoolgaDataParser.TAG_DOCUMENT);

				LOG.info(String.valueOf(nList.getLength()));
				countTokens.getAndAdd(nList.getLength());
				List<org.mpei.data.document.Document> docs = new ArrayList<org.mpei.data.document.Document>(
						nList.getLength());
				Log.info(String.valueOf(nList.getLength()));
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
						org.mpei.data.document.Document docData = DocumentFabric
								.newInstance();
						if (docData == null) {
							throw new RuntimeException("null docs");
						}
						docData.setContext(new Text(docNode.getTextContent()));
						docData.setYear(eElement.getElementsByTagName(TAG_YEAR)
								.item(0).getTextContent());
						docData.setAuthors(eElement
								.getElementsByTagName(TAG_AUTHORS).item(0)
								.getTextContent());
						docData.setName(eElement
								.getElementsByTagName(TAG_TITLE).item(0)
								.getTextContent());
						docData.setClassName(className);
						docs.add(docData);
					}
				}
				org.mpei.data.document.Document[] a = new org.mpei.data.document.Document[docs
						.size()];
				
					model.addDocuments(className, docs.toArray(a));
			} catch (Exception e) {
				e.printStackTrace();
			}

			LOG.info("Complete thread: " + file);
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
