package com.ntu.bdm.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class DummyMapper extends Mapper<Object, Text, Text, FloatWritable> {

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        String[] line = value.toString().split("\\t");
        context.write(new Text(line[0]), new FloatWritable(Float.parseFloat(line[1])));
    }
}
