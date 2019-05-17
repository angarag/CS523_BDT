package bdt.mars.hadoop.temp.average.P4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import bdt.mars.hadoop.temp.average.CustomPair;
import bdt.mars.hadoop.temp.average.CustomYear;

public class Reducer_InMapperCombining_DescendingYear extends
		Reducer<CustomYear, CustomPair, CustomYear, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();

	@Override
	public void setup(Context context) {
		// System.out.println("Reducer started");
	}

	@Override
	public void reduce(CustomYear key, Iterable<CustomPair> values,
			Context context) throws IOException, InterruptedException {
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
	// @Override
	// public void cleanup(Context context) throws IOException,
	// InterruptedException{
	// context.write(new Text("done"),result);
	// System.out.println("Reducer: I'm done");
	// }
}