package com.leoliu1221.hadoop.Temperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class MyReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {
 
    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxTemp = 0;
        for (IntWritable value : values) {
        	if (value.get()>maxTemp){
        		maxTemp = value.get();
        	}
        }
        context.write(text, new IntWritable(maxTemp));
      //  System.out.println("year "+text.toString()+"maxtemp:"+maxTemp);
    }
}