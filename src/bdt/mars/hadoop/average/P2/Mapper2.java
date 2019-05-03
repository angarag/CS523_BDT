package bdt.mars.hadoop.average.P2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import bdt.mars.hadoop.average.CustomPair;

public class Mapper2 extends
		Mapper<LongWritable, Text, Text, CustomPair> {

	private Text yeart = new Text();
	private CustomPair pair;
	private double tmp;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		String year = val.substring(15, 19);
		yeart.set(year);
		double temperature = Double.parseDouble(val.substring(87, 92)) / 10;
			pair = new CustomPair();
			pair.setTemp(temperature);
			pair.setCount(1);
		context.write(yeart,pair);
	}


}
