package bdt.mars.hadoop.temp.custom_sorted;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer_Custom extends Reducer<CustomKey, Text, CustomKey, Text> {

	@Override
	public void setup(Context context) {
		System.out.println("Reducer started");
	}

	@Override
	public void reduce(CustomKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text val : values) {
			System.out.println(key + " " + val);
			context.write(key, val);
		}

	}
}