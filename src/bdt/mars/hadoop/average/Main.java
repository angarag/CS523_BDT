package bdt.mars.hadoop.average;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.MapWritable;

import bdt.mars.hadoop.average.P1.Mapper_Regular;
import bdt.mars.hadoop.average.P1.Reducer_Regular;
import bdt.mars.hadoop.average.P2.Combiner;
import bdt.mars.hadoop.average.P2.Mapper2;
import bdt.mars.hadoop.average.P2.Reducer2;
import bdt.mars.hadoop.average.P3.Mapper_InMapperCombining;
import bdt.mars.hadoop.average.P3.Reducer_InMapperCombining;
import bdt.mars.hadoop.average.P4.Mapper_InMapperCombining_DescendingYear;
import bdt.mars.hadoop.average.P4.Reducer_InMapperCombining_DescendingYear;

public class Main extends Configured implements Tool {

	public static String option = "a";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		int res = ToolRunner.run(conf, new Main(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(Main.class);
		option = args[2];
		System.out.println("Option " + option + " passed");
		switch (option) {
		case "1":
			job.setMapperClass(Mapper_Regular.class);
			job.setReducerClass(Reducer_Regular.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			break;
		case "2":
			job.setMapperClass(Mapper2.class);
			job.setReducerClass(Reducer2.class);
			job.setCombinerClass(Combiner.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CustomPair.class);
			break;
		case "3":
			job.setMapperClass(Mapper_InMapperCombining.class);
			job.setReducerClass(Reducer_InMapperCombining.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CustomPair.class);
			break;
		case "4":
			job.setJarByClass(CustomPair.class);
			job.setJarByClass(CustomYear.class);
			job.setMapperClass(Mapper_InMapperCombining_DescendingYear.class);
			job.setReducerClass(Reducer_InMapperCombining_DescendingYear.class);
			// job.setMapOutputKeyClass(CustomYear.class);
			// job.setMapOutputValueClass(CustomPair.class);
			job.setOutputKeyClass(CustomYear.class);
			job.setOutputValueClass(CustomPair.class);
			break;
		case "5":
			job.setNumReduceTasks(2);
			job.setMapperClass(Mapper_InMapperCombining_DescendingYear.class);
			job.setReducerClass(Reducer_InMapperCombining_DescendingYear.class);
			// set partitioner statement
			job.setPartitionerClass(CustomPartitioner.class);
			job.setOutputKeyClass(CustomYear.class);
			job.setOutputValueClass(CustomPair.class);
			break;
		default:// 1,4,5
			System.out.println("default option passed");
			job.setMapperClass(Mapper_Regular.class);
			job.setReducerClass(Reducer_Regular.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			break;
		}

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}