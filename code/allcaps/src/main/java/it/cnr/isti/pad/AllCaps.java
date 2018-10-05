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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AllCaps extends Configured implements Tool
{
   public static class Map extends Mapper<LongWritable, Text, LongWritable, Text>
   {
      /**
       * The map() method capitalizes the line and emits it with the same key.
       */
      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
      {
         value.set(value.toString().toUpperCase());
         context.write(key, value);
      }
   }

   public static class Reduce extends Reducer<LongWritable, Text, Text, NullWritable>
   {
      /**
       * The reduce() method emits the value as the key and nothing as the value.
       */
      @Override
      public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
      {
         for (Text value : values) {
            context.write(value, NullWritable.get());
         }
      }
   }

   public static void main(String[] args) throws Exception
   {
      int res = ToolRunner.run(new Configuration(), new AllCaps(), args);

      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      Job job = Job.getInstance(getConf());

      job.setJarByClass(AllCaps.class);

      // Set map key and value classes
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(Text.class);

      // Set reduce key and value classes
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);

      // Set map and reduce implementing classes
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      // Set the reducers to 2
      job.setNumReduceTasks(2);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      return job.waitForCompletion(true) ? 0 : 1;
   }
}
