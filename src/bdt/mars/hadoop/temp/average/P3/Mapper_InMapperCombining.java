package bdt.mars.hadoop.temp.average.P3;

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

import bdt.mars.hadoop.temp.average.CustomPair;

public class Mapper_InMapperCombining extends
		Mapper<LongWritable, Text, Text, CustomPair> {

	private Text yeart = new Text();
	private CustomPair pair;
	private HashMap<String, CustomPair> myMap = new HashMap<String, CustomPair>();
	private double tmp;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		String year = val.substring(15, 19);
		yeart.set(year);
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
		// System.out.println(year + ": " + pair + year + "&" + temperature);
		myMap.put(year, pair);
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		System.out.println("InMapperCombining: Emitting the hashmap");
		for (String w : myMap.keySet()) {
			System.out.println(w + ":" + myMap.get(w));
			yeart.set(w);
			context.write(yeart, (CustomPair) myMap.get(w));
		}

	}
}
