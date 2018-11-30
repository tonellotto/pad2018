package it.cnr.isti.pad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopN_Driver 
{
	public static void main(String[] args) throws Exception 
	{
		if (args.length != 3) {
	         System.err.println("usage TopNDriver <N> <input> <output>");
	         System.exit(1);
	      }
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "TopN_Driver");
	    int N = Integer.parseInt(args[0]); // top N
	    job.getConfiguration().setInt("N", N);
	    job.setJobName("TopNDriver");
	    job.setJarByClass(TopN_Driver.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	    
		job.setMapperClass(TopN_Mapper.class);
		job.setReducerClass(TopN_Reducer.class);
		job.setNumReduceTasks(1);

		// map()'s output (K,V)
	    job.setMapOutputKeyClass(NullWritable.class);   
	    job.setMapOutputValueClass(Text.class);   
	      
	    // reduce()'s output (K,V)
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.setInputPaths(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));

	    job.waitForCompletion(true);
	}
}
