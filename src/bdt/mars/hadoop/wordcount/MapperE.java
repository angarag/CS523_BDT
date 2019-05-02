package bdt.mars.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperE extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());

		MapWritable myMap = new MapWritable();
		while (itr.hasMoreTokens()) {
			String ikey = itr.nextToken();
			Text itext = new Text(ikey);
			if (!myMap.containsKey(itext))
				myMap.put(itext, new IntWritable(1));
			else {
				IntWritable i = (IntWritable)myMap.get(itext);
				myMap.put(itext, new IntWritable(i.get()+1));
			}
		}
		for (Writable w : myMap.keySet()) {
			IntWritable wcount = (IntWritable)myMap.get(w);
			context.write((Text) w, wcount);
		}

	}
}
