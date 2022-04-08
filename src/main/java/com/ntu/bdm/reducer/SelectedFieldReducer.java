package com.ntu.bdm.reducer;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SelectedFieldReducer extends Reducer<Text, Text, Text, Text> {
    private int numCriterion = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.numCriterion = context.getConfiguration().getInt("numCriterion", 2);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // TODO Need to be dynamic = numMonths * numField
        int totalMonth = 12;
        KmeanFeature point = new KmeanFeature(totalMonth * numCriterion);
        for (Text value : values) {
            String[] v = value.toString().split("_");
            int idx = Integer.parseInt(v[0]);
            String trimmedVal = v[1].substring(1, v[1].length()-1);
            int ctr = 0;
            for (String s : trimmedVal.split(",")) {
                float f = Float.parseFloat(s);
                point.set(idx * totalMonth + ctr, f);
                ctr = (ctr + 1) % totalMonth;
            }
        }
        context.write(key, new Text(point.toString()));
    }
}
