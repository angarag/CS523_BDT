package bdt.mars.avro.AvroStationTempYear;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroMaxTemperature extends Configured implements Tool {

	private static Schema SCHEMA;

	public static class AvroMapper extends
			Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord record = new GenericData.Record(SCHEMA);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			utils.parse(value.toString());

			if (utils.isValidTemperature()) {
				record.put("stationId", utils.getStationId());
				record.put("temperature", utils.getAirTemperature());
				record.put("year", utils.getYearInt());
				System.out.println("mapper emitted"+record.toString());
				context.write(new AvroKey<GenericRecord>(record),
						NullWritable.get());

			}
		}
	}

	public static class AvroReducer
			extends
			Reducer<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {
		private String prevYear="";
		private String currentYear="";
		@Override
		protected void reduce(AvroKey<GenericRecord> key,
				Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println(key.toString());
			// Emit reducer output here
			currentYear=key.datum().get("year").toString();
			if(currentYear.compareTo(prevYear)!=0)
				context.write(key, NullWritable.get());// added by mars
			System.out.println(prevYear+" vs "+currentYear);
			prevYear=currentYear;

		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(AvroMaxTemperature.class);
		job.setJobName("Avro Station-Temp-Year");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String schemaFile = args[2];

		SCHEMA = new Schema.Parser().parse(new File(schemaFile));

		job.getConfiguration().setBoolean(
				Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);

		// Set Map output key and value classes here

		AvroJob.setMapOutputKeySchema(job, SCHEMA);
		AvroJob.setMapOutputValueSchema(job, SCHEMA);// added by mars
		// Set the schema for the reducer output here
		AvroJob.setOutputKeySchema(job, SCHEMA);// added by mars
		AvroJob.setOutputValueSchema(job, SCHEMA);// added by mars

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path("output"), true);
		int res = ToolRunner.run(conf, new AvroMaxTemperature(), args);
		System.exit(res);
	}
}