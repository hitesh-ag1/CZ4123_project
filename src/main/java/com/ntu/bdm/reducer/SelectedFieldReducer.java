package com.ntu.bdm.reducer;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SelectedFieldReducer extends Reducer<Text, Text, Text, Text> {
    private int numCriterion = 0;
    private int numMonthPerCriteria = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.numCriterion = context.getConfiguration().getInt("numCriterion", 2);
        this.numMonthPerCriteria = context.getConfiguration().getInt("numMonthPerCriteria", 12);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        KmeanFeature point = new KmeanFeature(numCriterion * numMonthPerCriteria);
        for (Text value : values) {
            System.out.println(value);
            System.out.println(numCriterion);
            String[] v = value.toString().split("_");
            int idx = Integer.parseInt(v[0]);
            String trimmedVal = v[1].substring(1, v[1].length() - 1);
            int ctr = 0;
            for (String s : trimmedVal.split(",")) {
                float f = Float.parseFloat(s);
                point.set(idx * numMonthPerCriteria + ctr, f);
                ctr = (ctr + 1) % numMonthPerCriteria;
            }
        }
        context.write(key, new Text(point.toString()));
    }
}
