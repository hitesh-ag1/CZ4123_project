package com.ntu.bdm.reducer;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class KmeanReducer extends Reducer<Text, Text, Text, KmeanFeature> {
    private int numYears;
    private int numMonths;
    private int numField;

    public void setup(Reducer.Context context) throws IOException, InterruptedException {
        this.numYears = context.getConfiguration().getInt("numYears", 5);
        this.numMonths = context.getConfiguration().getInt("numMonths", numYears * 12);
        this.numField = context.getConfiguration().getInt("numField", 2);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        KmeanFeature centroid = new KmeanFeature(numMonths * numField);
        ArrayList<KmeanFeature> points = new ArrayList<>();
        for (Text value : values) {
            String[] v = value.toString().split("_");
            String location = v[0];
            String[] strArr = v[1].split(",");
            points.add(new KmeanFeature(Arrays.toString(strArr)));
            context.write(new Text(key), points.get(points.size() - 1));
        }
        centroid.calculateCentroid(points);
    }
}

//# [...] is country A values
//L_CountryA_Cluster1 -> [...]
//# [***] is Centroid 1 values
//C_1 ->  [***]
