package it.cnr.isti.pad;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopN_Mapper extends Mapper<Object, Text, NullWritable, Text> 
{
	private int N = 10;
	private SortedMap<Integer, String> top = new TreeMap<Integer, String>();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		int weight = Integer.parseInt(value.toString().split("\\s+")[0]);
		top.put(weight, key.toString());
		
		// keep only top N
		if (top.size() > N) {
			top.remove(top.firstKey());
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		this.N = context.getConfiguration().getInt("N", 10); // default is top 10
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
	{
		for (String str : top.values()) {
			context.write(NullWritable.get(), new Text(str));
		}
	}
}