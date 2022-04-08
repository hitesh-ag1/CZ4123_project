package com.ntu.bdm.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MaxReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        System.out.println((key).toString());
        System.out.println((key).toString().equals("DATE"));
        if(!(key).toString().equals("DATE")){
            FloatWritable mx = new FloatWritable(Float.NEGATIVE_INFINITY);
            FloatWritable mn = new FloatWritable(Float.POSITIVE_INFINITY);
            for (Text value : values) {
                float floatVal = Float.parseFloat(value.toString());
                FloatWritable value2 = new FloatWritable(floatVal);
                float curVal = value2.get();
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
        else{
            // 2017-01
            IntWritable mx = new IntWritable(0);
            IntWritable mn = new IntWritable(300000);

            for (Text value : values) {
                int intVal = Integer.parseInt(value.toString().substring(0, 4) + value.toString().substring(5, 7));
                IntWritable value2 = new IntWritable(intVal);
                int curVal = value2.get();
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
}
