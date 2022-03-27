package com.ntu.bdm.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Objects;

public class MaxMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        String line = value.toString();
        String[] stringArr = line.split(",");
        String yearMonth = stringArr[1].substring(0, 7);
        String location = stringArr[0];
        if (stringArr[2].equals("M")) stringArr[2] = "NaN";
        if (stringArr[3].equals("M")) stringArr[3] = "NaN";
        float temp = Float.parseFloat(stringArr[2]);
        String humidity = stringArr[3];
        word.set(yearMonth + "-" + location);
        context.write(word, new FloatWritable(temp));
    }
}
