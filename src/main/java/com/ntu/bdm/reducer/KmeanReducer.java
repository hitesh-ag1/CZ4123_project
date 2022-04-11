package com.ntu.bdm.reducer;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class KmeanReducer extends Reducer<Text, Text, Text, KmeanFeature> {
    private int lengthOfFeature = 12;
    @Override
    protected void setup(Reducer<Text, Text, Text, KmeanFeature>.Context context) throws IOException, InterruptedException {
        this.lengthOfFeature = context.getConfiguration().getInt("lengthOfFeature", 12);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        KmeanFeature centroid = new KmeanFeature(lengthOfFeature);
        ArrayList<KmeanFeature> points = new ArrayList<>();
        System.out.printf("Key: %s \n",key);

        for (Text value : values) {
            System.out.printf("Value: %s \n",value);
            String v = value.toString();
            String[] strArr = v.split(",");
            String s = Arrays.toString(strArr);
            points.add(new KmeanFeature(s.substring(2, s.length()-2)));
        }

        centroid.calculateCentroid(points);
        context.write(new Text(key), centroid);
    }
}

//# [...] is country A values
//L_CountryA_Cluster1 -> [...]
//# [***] is Centroid 1 values
//C_1 ->  [***]
