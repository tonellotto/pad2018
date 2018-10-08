package it.cnr.isti.pad;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SearchWord extends Configured implements Tool
{
   public static void main(String[] args) throws Exception
   {
      int res = ToolRunner.run(new Configuration(), new SearchWord(), args);

      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception
   {
      if (args.length != 3) {
          System.out.println("Usage: ./SearchWord inputPath outputPath wordToSearch");
          System.exit(1);
      }

      Configuration conf = new Configuration();
      conf.set("wordToSearch", args[2]);

      Job job = new Job(conf, "searchWord");
      job.setJarByClass(SearchWord.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);

      job.setMapperClass(Map.class);

      // Map-only job, since the reducer has nothing to do.
      job.setNumReduceTasks(0);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static class Map extends Mapper<LongWritable, Text, Text, NullWritable>
   {
      private String wordToSearch;

      @Override
      public void setup(Context context)
      {
         Configuration conf = context.getConfiguration();
         this.wordToSearch = conf.get("wordToSearch");
      }

      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
      {
         if (value.toString().contains(wordToSearch)) {
            context.write(value, NullWritable.get());
         }
      }
   }
}
