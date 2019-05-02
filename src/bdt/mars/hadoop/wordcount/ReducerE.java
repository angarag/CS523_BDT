package bdt.mars.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ReducerE extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable total_distinct = new IntWritable(0);
	private int count=0;

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		count++;
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		total_distinct.set(count);
		context.write(new Text("The total number of distinct words is "), total_distinct);	

	}
}
