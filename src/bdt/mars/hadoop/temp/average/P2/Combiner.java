package bdt.mars.hadoop.temp.average.P2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import bdt.mars.hadoop.temp.average.CustomPair;

public class Combiner extends Reducer<Text, CustomPair, Text, CustomPair> {
	private CustomPair pair;

	@Override
	public void reduce(Text key, Iterable<CustomPair> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		double count = 0;
		System.out.println("Combiner invoked");
		for (CustomPair val : values) {
			sum += val.getTemp().get();
			count += val.getCount().get();
		}
		pair = new CustomPair(sum, count);
		context.write(key, pair);
	}
}