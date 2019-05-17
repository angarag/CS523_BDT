package bdt.mars.hadoop.temp.average.P1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper_Regular extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text year = new Text();
	private DoubleWritable temp = new DoubleWritable();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		String iyear = val.substring(15, 19);
		Double itemp = Double.parseDouble(val.substring(87, 92)) / 10;
		System.out.println(year + "-" + temp);
		year.set(iyear);
		temp.set(itemp);
		context.write(year, temp);
	}
}
