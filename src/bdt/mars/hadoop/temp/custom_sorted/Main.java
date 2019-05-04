package bdt.mars.hadoop.temp.custom_sorted;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.MapWritable;

import bdt.mars.hadoop.temp.average.P1.Mapper_Regular;
import bdt.mars.hadoop.temp.average.P1.Reducer_Regular;
import bdt.mars.hadoop.temp.average.P2.Combiner;
import bdt.mars.hadoop.temp.average.P2.Mapper2;
import bdt.mars.hadoop.temp.average.P2.Reducer2;
import bdt.mars.hadoop.temp.average.P3.Mapper_InMapperCombining;
import bdt.mars.hadoop.temp.average.P3.Reducer_InMapperCombining;
import bdt.mars.hadoop.temp.average.P4.Mapper_InMapperCombining_DescendingYear;
import bdt.mars.hadoop.temp.average.P4.Reducer_InMapperCombining_DescendingYear;

public class Main extends Configured implements Tool {

	public static String option = "a";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		int res = ToolRunner.run(conf, new Main(), args);
		fs.rename(new Path(args[1]+"/StationTempRecord-r-00000"), new Path(args[1]+"/StationTempRecord"));
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Custom sort for temperature");
		job.setJarByClass(Main.class);

		job.setMapperClass(Mapper_Custom.class);
		job.setReducerClass(Reducer_Custom.class);
		job.setOutputKeyClass(CustomKey.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.getConfiguration().set("mapreduce.output.basename", "StationTempRecord");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if(job.waitForCompletion(true)==true) {
//			FileSystem fs = FileSystem.get(new Configuration());
//			fs.rename(new Path(args[1]+"/part-r-00000"), new Path(args[1]+"/StationTempRecord"));
//			//fs.delete(new Path(args[1]), true);
			return 0;
		}else return 1;
	}
}