package com.ntu.bdm.reducer;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SelectedFieldReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // TODO Need to be dynamic = numMonths * numField
        KmeanFeature point = new KmeanFeature(12 * 2);
        for (Text value : values) {
            String[] v = value.toString().split("_");
            int idx = Integer.parseInt(v[0]);
            String trimmedVal = v[1].substring(1, v[1].length()-1);
            int ctr = 0;
            for (String s : trimmedVal.split(",")) {
                float f = Float.parseFloat(s);
                point.set(idx * 12 + ctr, f);
                ctr = (ctr + 1) % 12;
            }
        }
        context.write(key, new Text(point.toString()));
    }
}
