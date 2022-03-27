package com.ntu.bdm.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        String line = value.toString();
        String[] stringArr = line.split(",");
        String yearMonth = stringArr[1].substring(0, 7);
        String location = stringArr[2];
        float temp = Float.parseFloat(stringArr[3]);
        String humidity = stringArr[4];
        word.set(yearMonth + "-" + location);
        context.write(word, new FloatWritable(temp));
    }
}
