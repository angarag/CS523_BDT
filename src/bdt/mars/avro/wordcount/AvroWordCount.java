package bdt.mars.avro.wordcount;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroWordCount extends Configured implements Tool {

	public static class AvroWordCountMapper extends
			Mapper<LongWritable, Text, AvroKey<String>, AvroValue<Integer>> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (String token : value.toString().split("\\s+")) {
				context.write(new AvroKey<String>(token),
						new AvroValue<Integer>(1));
			}
		}
	}

	public static class AvroWordCountReducer
			extends
			Reducer<AvroKey<String>, AvroValue<Integer>, AvroKey<String>, AvroValue<Integer>> {

		public void reduce(AvroKey<String> key,
				Iterable<AvroValue<Integer>> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (AvroValue<Integer> value : values) {
				sum += value.datum();
			}

			context.write(key, new AvroValue<Integer>(sum));
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: WordCountTotalAvro <input path> <output path>");
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(AvroWordCount.class);
		job.setJobName("WordCountTotalAvro");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(AvroWordCountMapper.class);
		job.setReducerClass(AvroWordCountReducer.class);

		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);// corrected

		// Need to set mapper output key and value schema
		AvroJob.setMapOutputKeySchema(job, Schema.create(Type.STRING));
		AvroJob.setMapOutputValueSchema(job, Schema.create(Type.INT));

		// Need to set reducer output key and value schema
		AvroJob.setOutputKeySchema(job, Schema.create(Type.STRING));
		AvroJob.setOutputValueSchema(job, Schema.create(Type.INT));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path("output"), true);
		int res = ToolRunner
				.run(new Configuration(), new AvroWordCount(), args);
		System.exit(res);
	}
}