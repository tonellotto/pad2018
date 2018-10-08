package it.cnr.isti.pad;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FindBigram extends Configured implements Tool 
{   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
   {
      private final IntWritable ONE = new IntWritable(1);
      private final Text bigram = new Text();
      
      /**
       * The map() method emits every bigram in each line.
       */
      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
      {
         String[] words = value.toString().split("\\s+");
         String previous = null;
         
         for (String word: words) {
            if (word.length() > 0) {
               if (previous != null) {
                  bigram.set(previous + " " + word);
                  context.write(bigram, ONE);
               }

               previous = word;
            }
         }
      }
   }

   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
   {
      private String bigram = null;
      private int max = 0;
      
      /**
       * The reduce() method only finds the bigram with the largest count. Because
       * Hadoop will reuse the same Writable object for every iteration, we have to
       * store the values and not the objects.
       */
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         
         for (IntWritable value : values) {
            sum += value.get();
         }
         
         if (sum > max) {
            // Store the String, not the Text!
            bigram = key.toString();
            // Store the int, not the IntWritable!
            max = sum;
         }
      }

      /**
       * When we're all done, the cleanup() method emits the most common bigram.
       */
      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
         context.write(new Text(bigram), new IntWritable(max));
      }
   }

      public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new FindBigram(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      Job job = Job.getInstance(getConf());
      
      job.setJarByClass(FindBigram.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // Set the reducers to 2
      job.setNumReduceTasks(1);


      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      return job.waitForCompletion(true) ? 0 : 1;
   }

}
