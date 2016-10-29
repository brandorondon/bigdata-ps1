package framework.code.inverted;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import framework.util.StringIntegerList;
import framework.util.StringIntegerList.StringInteger;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringInteger> {


		@Override
		public void map(Text articleId, Text indices, Context context) throws IOException,
				InterruptedException {
			// Make a new string integer list which will contain (word, frequency) pairs
			StringIntegerList sil = new StringIntegerList();
			// Read the input line and parse it
			sil.readFromString(indices.toString());
			for (StringInteger index : sil.getIndices()) {
				StringInteger documentAndWordCount = new StringInteger(articleId.toString(), index.getValue());
				Text word = new Text(index.getString());
				// Send (word, (document_id, word_count)) pairs to reducers
				context.write(word, documentAndWordCount);
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			// Make a hashmap of (document, word_count) pairs for this specific "lemma"
			HashMap<String, Integer> docAndFreq = new HashMap<String, Integer>();
			for (StringInteger docIdAndLemma : articlesAndFreqs) {
				docAndFreq.put(docIdAndLemma.getString(), docIdAndLemma.getValue());
			}
			// Translate the hashmap into a string integer list
			StringIntegerList invertedIndex = new StringIntegerList(docAndFreq);
			context.write(lemma, invertedIndex);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (args.length != 2){
			System.err.println("Usage: <in> <out>");		
		}
		Job job = Job.getInstance(conf);
		job.setJarByClass(InvertedIndexMapred.class);
		job.setMapperClass(InvertedIndexMapper.class); 
		job.setReducerClass(InvertedIndexReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","\t");
		job.setMapOutputValueClass(StringInteger.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)? 0: 1);
		
	}
}
