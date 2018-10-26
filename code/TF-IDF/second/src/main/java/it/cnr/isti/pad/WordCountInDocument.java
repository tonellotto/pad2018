package it.cnr.isti.pad;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountInDocument 
{
	public static class NewMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	    {
	        String[] wordAndDocCounter = value.toString().split("\t");
	        String[] wordAndDoc = wordAndDocCounter[0].split("@");
	        context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "=" + wordAndDocCounter[1]));
	    }
	}
	
	public static class NewReducer extends Reducer<Text, Text, Text, Text> 
	{
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	    {
	    	int sumOfWordsInDocument = 0;
	    	Map<String, Integer> tempCounter = new HashMap<String, Integer>();
	    	for (Text val : values) {
	    		String[] wordCounter = val.toString().split("=");
	    		tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
	    		sumOfWordsInDocument += Integer.parseInt(val.toString().split("=")[1]);
    		} 
	    	for (String wordKey : tempCounter.keySet())
	    		context.write(new Text(wordKey + "@" + key.toString()), new Text(tempCounter.get(wordKey) + "/" + sumOfWordsInDocument));
	    }
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "word count in document");
		job.setJarByClass(WordCountInDocument.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(NewMapper.class);
		job.setReducerClass(NewReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
