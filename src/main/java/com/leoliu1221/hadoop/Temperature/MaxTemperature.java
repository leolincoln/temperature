package com.leoliu1221.hadoop.Temperature;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxTemperature {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = Job.getInstance(conf);
		job.setJarByClass(MaxTemperature.class);

		// Setup MapReduce
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}

	private static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

		
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			IntWritable wTemperature = new IntWritable();

			// check if the Temperature = 9999, it should be interpreted as
			// missing
			// value.
			String data = value.toString();
			// Temperature = positions at 87-­‐92
			Integer temp = Integer.parseInt(data.substring(87, 92));
			if (temp == 9999)
				return;

			// The temperature quality is in position 92-­‐93. If it is in the
			// range
			// {0,1,4,5,9}, then the
			// temperature reading is accurate and satisfactory.
			Integer acc = Integer.parseInt(data.substring(92, 93));
			Integer[] accList = { 0, 1, 4, 5, 9 };
			if (!Arrays.asList(accList).contains(acc))
				return;

			// Year = positions at 15-­‐19
			String year = data.substring(15, 19);
			word.set(year);
			wTemperature.set(temp);
			context.write(word, wTemperature);

		}

	}

	private static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text text, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int maxTemp = 0;
			for (IntWritable value : values) {
				if (value.get() > maxTemp) {
					maxTemp = value.get();
				}
			}
			context.write(text, new IntWritable(maxTemp));
			
		}
	}

}