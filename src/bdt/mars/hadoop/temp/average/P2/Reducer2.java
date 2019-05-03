package bdt.mars.hadoop.temp.average.P2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import bdt.mars.hadoop.temp.average.CustomPair;

public class Reducer2 extends Reducer<Text, CustomPair, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();

	@Override
	public void reduce(Text key, Iterable<CustomPair> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		double count = 0;

		for (CustomPair val : values) {
			sum += val.getTemp().get();
			count += val.getCount().get();
		}
		System.out.println(key.toString() + "-" + sum / count);
		result.set(sum / count);
		context.write(key, result);
	}
}