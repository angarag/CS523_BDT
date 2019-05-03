package bdt.mars.hadoop.temp.custom_sorted;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper_Custom extends Mapper<LongWritable, Text, CustomKey, Text> {

	private Text year = new Text();
	private CustomKey ckey ;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		String stationID = val.substring(4, 9) + "-" + val.substring(10, 14);
		String iyear = val.substring(15, 19);
		Double itemp = Double.parseDouble(val.substring(87, 92)) / 10;
		ckey = new CustomKey(stationID, itemp, iyear);
		//System.out.println(ckey+iyear);
		context.write(ckey, new Text(iyear));
	}
}
