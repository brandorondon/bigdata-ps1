package framework.code.articles;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import framework.util.WikipediaPageInputFormat;


/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {

	//@formatter:off
	/**
	 * Input:
	 * 		Page offset 	WikipediaPage
	 * Output
	 * 		Page offset 	WikipediaPage
	 * @author Tuan
	 *
	 */
	//@formatter:on
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement people articles load from
			// DistributedCache here
			super.setup(context);
			Configuration conf = context.getConfiguration();
		    Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		    BufferedReader reader = null;

		    for(Path path: localFiles){
		    	if (path.toString().endsWith("people.txt")){
		    		reader = new BufferedReader(new FileReader(path.toString()));
		    		break;
		    	}
		    }
		    String name;
		    while((name = reader.readLine()) != null){
		    	peopleArticlesTitles.add(name);
		    }
		    reader.close();			
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here
			Text title = new Text();
			Text content = new Text();
			
			if(inputPage.isArticle()){ 
				String currTitle = inputPage.getTitle();
				if (peopleArticlesTitles.contains(title)){
					title.set(currTitle);
					content.set(inputPage.getContent());
					context.write(title, content);
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf = new Configuration();
		//assuming we have people.txt at the hdfs root 
		DistributedCache.addCacheFile(new URI("people.txt"), conf);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2){
			System.err.println("Usage: <get articles jar> <in> <out>");		
		}
		Job job = new Job(conf, "Get Articles");
		job.setJarByClass(GetArticlesMapred.class);
		job.setMapperClass(GetArticlesMapper.class); 
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		//no shuffling, combining or any sort of reducing occurs
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)? 0: 1);	
	}
}
