package com.ntu.bdm.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MeanReducer extends Reducer<Text, FloatWritable, Text, Text> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0F;
        float sqrSum = 0F;
        int count = 0;
        List<Float> list = new ArrayList<>();
        for (FloatWritable value : values) {
            float curVal = value.get();
            // TODO - Need to exclude NaN in calculation 100+NaN = NaN
            if (!Float.isNaN(curVal)){
                sum += curVal;
                count += 1;
                list.add(curVal);
            }

        }

        Collections.sort(list);
        int len = list.size();
        float median;
        if (len % 2 != 0) {
            median = list.get(len / 2);
        } else {
            median = (list.get(len / 2) + list.get(len / 2 - 1)) / 2F;
        }

        float mean = sum / count;

        for (float f : list){
            float diff = f - mean;
            sqrSum += diff * diff;
        }

        float sd = (float) Math.sqrt(sqrSum / count);


        System.out.printf("Key: %s\n", key);
        System.out.printf("Mean: %s\n", mean);
        System.out.printf("Median: %s\n", median);
        System.out.printf("SD: %s\n", sd);

        String output = String.format("%s,%s,%s", mean, median,sd);
        context.write(key, new Text(output));
    }
}
