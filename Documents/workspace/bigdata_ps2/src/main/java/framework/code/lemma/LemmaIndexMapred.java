package framework.code.lemma;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import framework.util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * 
 *
 */
public class LemmaIndexMapred {

	
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {
		protected static Tokenizer tokenizer = new Tokenizer();
		protected static Properties props = new Properties();
		protected static StanfordCoreNLP pipeline;
		private Text docID = new Text();
		
		public LemmaIndexMapper(){
			super();
			this.props.put("annotators", "tokenize, ssplit, pos, lemma");
			this.pipeline = new StanfordCoreNLP(props);
		}
		
		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			String content = page.getContent();
			String cleanedDoc = tokenizer.tokenize(content);
						
			List<String> lemmas = getLemmas(cleanedDoc);
			Map<String, Integer> lemmaMap = new HashMap<String, Integer>();
			for(String lemma : lemmas){
				if(lemmaMap.containsKey(lemma)){
					lemmaMap.put(lemma, lemmaMap.get(lemma) + 1);
				} else {
					lemmaMap.put(lemma, 1);
				}
			}
			
			docID.set(page.getDocid()); //or title instead?
			StringIntegerList result = new StringIntegerList(lemmaMap);
			context.write(docID, result);					
		}
		
		private List<String> getLemmas(String doc){
			List<String> lemmas = new LinkedList<String>();
	        Annotation document = new Annotation(doc);
	        this.pipeline.annotate(document);

	        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
	        for(CoreMap sentence: sentences) {
	            // Iterate over all tokens in a sentence
	            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
	                // Retrieve and add the lemma for each word into the
	                // list of lemmas
	                lemmas.add(token.get(LemmaAnnotation.class));
	            }
	        }
	        return lemmas;
		}
	}
}
