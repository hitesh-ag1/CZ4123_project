package com.ntu.bdm.reducer;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PointReducer extends Reducer<Text, Text, Text, Text> {
    private int numMonth;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.numMonth = context.getConfiguration().getInt("numMonth", 12);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        KmeanFeature point = new KmeanFeature(numMonth);
        for (Text value : values) {
            String[] v = value.toString().split("_");
            int idx = Integer.parseInt(v[0]);
            float f = Float.parseFloat(v[1]);
            point.set(idx, f);
        }
        context.write(key, new Text(point.toString()));
    }
}
