package com.ntu.bdm.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MaxReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterator<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float max = 0;
        while (values.hasNext()) {
            float curVal = values.next().get();
            if (curVal > max) {
                max = curVal;
            }
        }
        context.write(key, new FloatWritable(max));
    }

}
