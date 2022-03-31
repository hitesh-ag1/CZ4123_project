package com.ntu.bdm.reducer;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SelectedFieldReducer extends Reducer<Text, Text, Text, KmeanFeature> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // TODO Need to be dynamic = numMonths * numField
        KmeanFeature point = new KmeanFeature(60 * 2);
        for (Text value : values) {
            String[] v = value.toString().split("_");
            int idx = Integer.parseInt(v[0]);
            int ctr = 0;
            for (String s : v[1].split(",")) {
                float f = Float.parseFloat(s);
                point.set(idx * 60 + ctr, f);
                ctr = (ctr + 1) % 60;
            }
        }
        context.write(key, point);
    }
}
