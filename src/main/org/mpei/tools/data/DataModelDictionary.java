package org.mpei.tools.data;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class DataModelDictionary implements Dictionary {
	private DataModel model;
	private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);

	public DataModelDictionary(DataModel model) {
		this.model = model;
	}

	public static class MyAnalyzer extends Analyzer {
		public final TokenStream tokenStream(String fieldName, Reader reader) {
			return new PorterStemFilter(new LowerCaseTokenizer(
					Version.LUCENE_36, reader));
		}
	}

	public String[] getWords() {
		List<String> result = new ArrayList<String>();
		for (String key : model.getLabels()) {
			for (Document doc : model.getDocuments(key)) {
				try {
					TokenStream stream = analyzer.tokenStream(null,
							new StringReader(doc.getContext().toString()));
					while (stream.incrementToken()) {
						result.add(stream.getAttribute(CharTermAttribute.class)
								.toString());
					}
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
			}
		}
		return result.toArray(new String[0]);
	}

	public String[] getStemmWords() {
		String[] words = getWords();
		String[] result = new String[words.length];
		int i = 0;
		MyAnalyzer a = new MyAnalyzer();
		for (String word : words) {
			try {
				TokenStream stream = a
						.tokenStream(null, new StringReader(word));
				while (stream.incrementToken()) {
					result[i] = stream.getAttribute(CharTermAttribute.class)
							.toString();
					++i;
				}
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			} finally {
				a.close();
			}
		}
		return result;
	}

	public static void main(String[] args) {
		DataModelBuilder builder = new JsonUrlDataModelBuilder();
		DataModel model = DataModel.read("dataModel");
		Dictionary d = new DataModelDictionary(model);
		for (String word : d.getWords()) {
			System.out.println(word);
		}
		for (String word : d.getStemmWords()) {
			System.out.println(word);
		}
	}

}
