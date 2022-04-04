package com.ntu.bdm.reducer;

import com.ntu.bdm.util.KmeanFeature;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.checkerframework.checker.units.qual.K;

import java.io.IOException;
import java.util.ArrayList;

public class KmeanReducer extends Reducer<Text, Text, Text, KmeanFeature> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // TODO Need to be dynamic = numMonths * numField
        KmeanFeature centroid = new KmeanFeature(60 * 2);
        ArrayList<KmeanFeature> points = new ArrayList<>();
        for (Text value : values) {
            String[] v = value.toString().split("_");
            String location = v[0];
            String[] strArr = v[1].split(",");
            points.add(new KmeanFeature(Arrays.toString(strArr)));
            context.write(new Text( "L_"+ location + "_" + key), points.get(points.size() - 1));
        }
        centroid.calculateCentroid(points);
        context.write(new Text("C_" + key), centroid);
    }
}
