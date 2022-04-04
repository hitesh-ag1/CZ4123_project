package com.ntu.bdm.reducer;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class OutputReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(new Text(key), new Text(values.toString()));
    }
}

//# [...] is country A values
//L_CountryA_Cluster1 -> [...]
//# [***] is Centroid 1 values
//C_1 ->  [***]
