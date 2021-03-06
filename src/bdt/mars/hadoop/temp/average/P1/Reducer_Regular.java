package bdt.mars.hadoop.temp.average.P1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer_Regular extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double sum = 0;
		double count = 0;

		for (DoubleWritable val : values) {
			sum += val.get();
			count += 1;
		}
		System.out.println(key.toString() + "-" + sum / count);
		result.set(sum / count);
		context.write(key, result);
	}
}