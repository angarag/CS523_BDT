package bdt.mars.hadoop.average;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

	public  class RegularMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String val = value.toString();
		String year = val.substring(15, 19);
		Double temperature = Double.parseDouble(val.substring(87, 92));
			word.set(year);
			context.write(word, new DoubleWritable(temperature));
	}
}
