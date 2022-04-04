package com.ntu.bdm.mapper;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class OutputMapper extends Mapper<Object, Text, Text, Text> {
    private KmeanFeature[] centroids;
    private int numCluster;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        this.numCluster = context.getConfiguration().getInt("numCluster", 3);
        this.centroids = new KmeanFeature[numCluster];
        for (int i = 0; i < numCluster; i++){
            String[] s = context.getConfiguration().getStrings("centroid-" + i);
            String str = Arrays.toString(s);
            System.out.println(str);
            this.centroids[i] = new KmeanFeature(str.substring(2, str.length()-2));
        }
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        System.out.printf("Key: %s \n",key);
        System.out.printf("Value: %s \n",value);
        String[] keyVal = value.toString().split("\\t");

        String location = keyVal[0];

        String line = keyVal[1];
        String[] stringArr = line.split(",");
        KmeanFeature point = new KmeanFeature(stringArr.length);

        for (int i = 0; i < stringArr.length; i++){
            point.set(i, Float.parseFloat(stringArr[i].substring(1, stringArr[i].length()-1)));
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
        Text newVal = new Text(String.format("%s", location));
        context.write(new Text(String.valueOf(nearestCentroid)), newVal);
    }

}
