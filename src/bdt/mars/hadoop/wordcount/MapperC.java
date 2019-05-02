package bdt.mars.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperC extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		for (String token : value.toString().split("\\s+")) {
			// Q2-c
			token = token.toLowerCase();
			if (token.equals("hadoop") || token.equals("java")) {
				word.set(token);
				context.write(word, one);
			}
		}
	}
}
