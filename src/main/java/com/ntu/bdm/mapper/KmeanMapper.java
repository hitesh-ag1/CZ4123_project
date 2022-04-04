package com.ntu.bdm.mapper;

import com.ntu.bdm.util.KmeanFeature;

import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

public class KmeanMapper extends Mapper<Object, Text, IntWritable, Text> {
    private KmeanFeature[] centroids;
    private int numCluster;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        this.numCluster = context.getConfiguration().getInt("numCluster", 3);
        this.centroids = new KmeanFeature[numCluster];
        for (int i = 0; i < numCluster; i++){
            String[] s = context.getConfiguration().getStrings("centroid." + i);
            this.centroids[i] = new KmeanFeature(Arrays.toString(s));
        }
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        // TODO - Check how to read into key val pairs
        String[] keyVal = value.toString().split("\\t");

        String[] oldKey = keyVal[0].split("_");
        String location = oldKey[1];

        String line = keyVal[1];
        String[] stringArr = line.split(",");
        KmeanFeature point = new KmeanFeature(stringArr.length);

        for (int i = 0; i < stringArr.length; i++){
            point.set(i, Float.parseFloat(stringArr[i]));
        }

        float min = Float.POSITIVE_INFINITY;
        float curDist = 0F;
        int nearestCentroid = -1;

        for (int i = 0; i < centroids.length; i++) {
            curDist = point.distance(centroids[i]);

            if (curDist < min){
                nearestCentroid = i;
                min = curDist;
            }
        }
        Text newVal = new Text(String.format("%s_%s", location, point));
        context.write(new IntWritable(nearestCentroid), newVal);
    }
    
    private float[][] generateCentroid(int len, int numClus){
        float[][] centroid = new float[numClus][len];
        HashMap<Float, Boolean> position = new HashMap<>();

        int size = 0;
        while (size < len * numClus) {
            Float randFloat = (float) Math.random();
            if (!position.containsKey(randFloat)) {
                position.put(randFloat, true);
                centroid[size / len][size % len] = randFloat;
                size += 1;
            }
        }
        return centroid;
    }
}
