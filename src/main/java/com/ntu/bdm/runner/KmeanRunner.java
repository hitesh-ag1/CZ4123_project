package com.ntu.bdm.runner;

import com.ntu.bdm.mapper.KmeanMapper;
import com.ntu.bdm.reducer.KmeanReducer;
import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.checkerframework.checker.units.qual.K;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;


/*
 * Input k,v = SG: 0.1, 0.3, 0.7.....
 * The mapper will choose the nearest centroid
 *
 * Map to k,v = 1: 0.1,0.2...
 *              1: 0.5,0.6....
 *
 * Output k,v = 1: 0.1, 0.3, 0.7....., 0.4, 0.5, 0.6....
 * v will be the new centroid
 *
 *
 */

public class KmeanRunner {

    public KmeanRunner(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://3.1.36.136:9000");
        conf.set("yarn.resourcemanager.hostname", "3.1.36.136"); // see step 3
        conf.set("mapreduce.framework.name", "yarn");

        boolean stop = false;

        // TODO - Pass in command line arguments
        int numCluster = 3;
        int numYear = 1;
        int numField = 2;
        int lengthOfFeatures = 12 * numField * numYear;
        int numIteration = 2;

        conf.setInt("numCluster", numCluster);
        conf.setInt("numYear", numYear);
        conf.setInt("numField", numField);
        conf.setInt("lengthOfFeatures", lengthOfFeatures);

        KmeanFeature[] newCentroid = generateCentroid(lengthOfFeatures, numCluster);

        for (int i=0; i < numCluster; i++){
            conf.unset("centroid-" + i);
            conf.set("centroid-" + i, newCentroid[i].toString());
        }


        int ctr = 0;
        while (!stop) {
            Job job = Job.getInstance(conf, "Kmean_" + ctr);

            job.setJarByClass(KmeanRunner.class);
            job.setMapperClass(KmeanMapper.class);
            job.setReducerClass(KmeanReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem filesystem = FileSystem.get(conf);
            filesystem.delete(new Path(outPath), true);

            FileInputFormat.addInputPath(job, new Path(inPath));
            FileOutputFormat.setOutputPath(job, new Path(outPath));
            boolean status = job.waitForCompletion(true);

            if (!status) {
                System.err.println("Iteration " + ctr + " failed");
                System.exit(1);
            }

            KmeanFeature[] tmp = new KmeanFeature[numCluster];
            for (int i =0; i < numCluster; i++){
                tmp[i] = KmeanFeature.duplicate(newCentroid[i]);
            }
            newCentroid = readCentroid(outPath, conf, numCluster, tmp);
            System.out.println(ctr);
            System.out.println(Arrays.toString(newCentroid));


            // TODO - Threshold to stop based on distance
            stop = ctr >= numIteration-1;
            for (int i=0; i < numCluster; i++){
                conf.unset("centroid-" + i);
                conf.set("centroid-" + i, newCentroid[i].toString());
            }

            ctr += 1;

        }
    }

    private KmeanFeature[] generateCentroid(int len, int numClus) {
        KmeanFeature[] centroids = new KmeanFeature[numClus];
        for (int i = 0 ; i < numClus; i++ ) centroids[i] = new KmeanFeature(len);
        HashMap<Float, Boolean> position = new HashMap<>();

        int size = 0;
        while (size < len * numClus) {
            Float randFloat = (float) Math.random() * 100;
            if (!position.containsKey(randFloat)) {
                position.put(randFloat, true);
                centroids[size / len].set(size % len, randFloat);
                size += 1;
            }
        }
        return centroids;
    }

    private KmeanFeature[] readCentroid(String inPath, Configuration conf,int numCluster, KmeanFeature[] old) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] statuses = hdfs.listStatus(new Path(inPath));

        for (int i = 0; i < statuses.length; i++){
           if(!statuses[i].getPath().toString().endsWith("_SUCCESS")){
               BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(statuses[i].getPath())));
               while (br.readLine() != null) {
                   String[] line = br.readLine().split("\\t");
                   int centroidId = Integer.parseInt(line[0]);
                   String centroid = line[1];
                   old[centroidId] = new KmeanFeature(centroid.substring(1, centroid.length() - 1));
               }
               br.close();
           }
        }

        return old;
    }
}
