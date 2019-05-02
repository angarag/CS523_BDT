package bdt.mars.hadoop.average;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Combiner extends Reducer<Text, Text, Text, Text> {
	private Text pair = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		double count = 0;
		System.out.println("Combiner invoked");
		for (Text val : values) {
			String[] arr = val.toString().split(",");
			sum += Double.parseDouble(arr[0]);
			count += Double.parseDouble(arr[1]);
		}
		System.out.println(key.toString() + ":" + sum + "," + count);
		pair.set(new String(sum + "," + count));
		context.write(key, pair);
	}
}