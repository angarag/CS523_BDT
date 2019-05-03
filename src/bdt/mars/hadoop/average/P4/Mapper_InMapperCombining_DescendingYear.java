package bdt.mars.hadoop.average.P4;

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
import bdt.mars.hadoop.average.CustomYear;

public class Mapper_InMapperCombining_DescendingYear extends
		Mapper<LongWritable, Text, CustomYear, CustomPair> {

	private CustomPair pair;
	private CustomYear cyear;
	private HashMap<CustomYear, CustomPair> myMap = new HashMap<CustomYear, CustomPair>();
	private double tmp;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		String year = val.substring(15, 19);
		cyear = new CustomYear(year);
		double temperature = Double.parseDouble(val.substring(87, 92)) / 10;
		// System.out.println(year+"-"+temp);

		if (myMap.containsKey(cyear)) {
			pair = myMap.get(cyear);
			temperature += pair.getTemp().get();
			pair.setTemp(temperature);
			pair.setCount(pair.getCount().get() + 1);
		} else {
			pair = new CustomPair();
			pair.setTemp(temperature);
			pair.setCount(1);
		}
		// System.out.println(yeart+": "+pair+year+"&"+temperature);
		myMap.put(cyear, pair);
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {

		for (CustomYear w : myMap.keySet()) {
			System.out.println(w + ":" + myMap.get(w));
		}
		System.out.println("InMapperCombining with descending years: Finished emitting the years");
		for (CustomYear w : myMap.keySet()) {
			 context.write(new CustomYear(w.getYear().toString()),  myMap.get(w));
			// System.out.println("emitting to reducer");
		}
		
	}
}
