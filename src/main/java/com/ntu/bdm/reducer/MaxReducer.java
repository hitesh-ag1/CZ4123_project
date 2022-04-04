package com.ntu.bdm.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MaxReducer extends Reducer<Text, FloatWritable, Text, Text> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        FloatWritable mx = new FloatWritable(Float.NEGATIVE_INFINITY);
        FloatWritable mn = new FloatWritable(Float.POSITIVE_INFINITY);
        for (FloatWritable value : values) {
            float curVal = value.get();
            if (curVal > mx.get()) {
                mx.set(curVal);
            }

            if (curVal < mn.get()) {
                mn.set(curVal);
            }
        }
        System.out.printf("Key: %s\n", key);
        System.out.printf("Max: %s\n",mx);
        System.out.printf("Min: %s\n",mx);
        String output = String.format("%s,%s",mx,mn);
        context.write(key, new Text(output));
    }
}
