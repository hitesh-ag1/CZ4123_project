package com.ntu.bdm.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class PointMapper extends Mapper<Object, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        String[] keyVal = value.toString().split("\\t");
        String line = keyVal[1];

        String[] oldKey = keyVal[0].split("_");
        String idx = oldKey[0];
        String location = oldKey[1];
        String field = oldKey[2];

        String[] stats = {"MAX", "MIN"};
        String[] stringArr = line.split(",");

        if (stringArr.length == 3) {
            stats = new String[]{"MEAN", "MEDIAN", "SD"};
        }

        for (int i = 0; i < stringArr.length; i++) {
            float f = Float.parseFloat(stringArr[i]);
            String stat = stats[i];
            String newKey = String.format("%s_%s_%s", location, field, stat);
            String newVal = String.format("%s_%f", idx, f);
            context.write(new Text(newKey), new Text(newVal));
        }
    }
}
