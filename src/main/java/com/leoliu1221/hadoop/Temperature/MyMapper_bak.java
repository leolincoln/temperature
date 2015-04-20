package com.leoliu1221.hadoop.Temperature;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper_bak extends Mapper<Object, Text, Text, IntWritable> {

	private final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		IntWritable wTemperature = new IntWritable();

		// check if the Temperature = 9999, it should be interpreted as missing
		// value.
		String data = value.toString();
		// Temperature = positions at 87-­‐92
		Integer temp = Integer.parseInt(data.substring(88, 92));
		if(data.charAt(87)=='-') temp = -1*temp;
		if (temp == 9999)
			return;

		// The temperature quality is in position 92-­‐93. If it is in the range
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