package com.ntu.bdm.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class OutputReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> s = new ArrayList<>();
        for (Text value : values) {
            s.add(value.toString());
        }
        context.write(new Text(key), new Text(s.toString()));
    }
}
