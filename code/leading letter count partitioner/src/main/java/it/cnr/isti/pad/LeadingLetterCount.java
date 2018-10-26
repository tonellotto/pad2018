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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LeadingLetterCount extends Configured implements Tool
{
    public enum StartsWith
    {
        STARTS_WITH_CONSONANT,
        STARTS_WITH_VOWEL
    }

    static boolean startsWithVowel(String word) {
        return "aeiou".contains(word.toLowerCase().substring(0, 1));
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            for (String token : value.toString().split("\\s+")) {
                if ((token.length() > 0) && token.matches("^[A-Za-z].*")) {
                    word.set(token);
                    context.write(word, ONE);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private String word = null;
        private int best = 0;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum > best) {
                word = key.toString();
                best = sum;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(word), new IntWritable(best));
        }
    }

    public static class LeadingLetterPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return startsWithVowel(key.toString()) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new LeadingLetterCount(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "LeadingLetterCount");
        job.setJarByClass(LeadingLetterCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setPartitionerClass(LeadingLetterPartitioner.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(2);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;

    }
}
