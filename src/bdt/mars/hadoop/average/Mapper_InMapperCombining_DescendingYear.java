package bdt.mars.hadoop.average;

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

public class Mapper_InMapperCombining_DescendingYear extends
		Mapper<LongWritable, Text, CustomYear, CustomPair> {

	private CustomYear yeart;
	private CustomPair pair;
	private HashMap<String, CustomPair> myMap = new HashMap<String, CustomPair>();
	private double tmp;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		String year = val.substring(15, 19);
		yeart = new CustomYear(year);
		double temperature = Double.parseDouble(val.substring(87, 92)) / 10;
		// System.out.println(year+"-"+temp);

		if (myMap.containsKey(year)) {
			pair = myMap.get(year);
			temperature += pair.getTemp().get();
			pair.setTemp(temperature);
			pair.setCount(pair.getCount().get() + 1);
		} else {
			pair = new CustomPair();
			pair.setTemp(temperature);
			pair.setCount(1);
		}
		//System.out.println(yeart+": "+pair+year+"&"+temperature);
		myMap.put(year, pair);
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		System.out.println("InMapperCombining with descending years: Emitting the hashmap");
		for (String w : myMap.keySet()) {
			System.out.println(w + ":" + myMap.get(w));
			context.write(new CustomYear(w), (CustomPair) myMap.get(w));
		}

	}
}
