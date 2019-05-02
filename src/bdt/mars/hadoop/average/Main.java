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

		switch (option) {
		case "2":
			job.setMapperClass(RegularMapper.class);
			job.setReducerClass(RegularReducer.class);
			job.setCombinerClass(Combiner.class);
			break;
		case "3":
			job.setMapperClass(InMapperCombining.class);
			job.setReducerClass(ReducerForInMapperCombining.class);
			job.setOutputValueClass(CustomPair.class);
			break;
		default:// 1,4,5
			System.out.println("default option passed");
			job.setMapperClass(RegularMapper.class);
			job.setReducerClass(RegularReducer.class);
			job.setOutputValueClass(DoubleWritable.class);
			break;
		}

		job.setOutputKeyClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		// Q2-b
		if (option.equals("5"))
			job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}