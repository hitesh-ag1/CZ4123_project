package com.ntu.bdm.runner;

import com.ntu.bdm.mapper.KmeanMapper;
import com.ntu.bdm.reducer.KmeanReducer;
import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.InetAddress;
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

    public KmeanRunner(String inPath, String outPath, int numIteration) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        if (inPath.isEmpty()) inPath = "/CZ4123/selected";
        if (outPath.isEmpty()) outPath = "/CZ4123/kmean";
        String ip = InetAddress.getLocalHost().toString().split("/")[1];
        conf.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf.set("mapreduce.framework.name", "yarn");

        boolean stop = false;

        // TODO - Pass in command line arguments
        int numCluster = 3;
        int numYear = 1;
        int numField = 2;
        int lengthOfFeatures = 12 * numField * numYear;

        conf.setInt("numCluster", numCluster);
        conf.setInt("numYear", numYear);
        conf.setInt("numField", numField);
        conf.setInt("lengthOfFeatures", lengthOfFeatures);

        KmeanFeature[] newCentroid = generateCentroid(lengthOfFeatures, numCluster);

        for (int i = 0; i < numCluster; i++) {
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
            for (int i = 0; i < numCluster; i++) {
                tmp[i] = KmeanFeature.duplicate(newCentroid[i]);
            }
            newCentroid = readCentroid(outPath, conf, numCluster, tmp);
            System.out.println(ctr);
            System.out.println(Arrays.toString(newCentroid));


            // TODO - Threshold to stop based on distance
            stop = ctr >= numIteration - 1;
            for (int i = 0; i < numCluster; i++) {
                conf.unset("centroid-" + i);
                conf.set("centroid-" + i, newCentroid[i].toString());
            }
            ctr += 1;
        }
        writeCentroid(conf, newCentroid, "/test/centroid");

    }

    private static void writeCentroid(Configuration conf, KmeanFeature[] centroids, String output) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(output + "/centroids.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

        //Write the result in a unique file
        for (int i = 0; i < centroids.length; i++) {
            br.write(i + "\t" + centroids[i].toString());
            br.newLine();
        }

        br.close();
        hdfs.close();
    }

    private KmeanFeature[] generateCentroid(int len, int numClus) {
        KmeanFeature[] centroids = new KmeanFeature[numClus];
        for (int i = 0; i < numClus; i++) centroids[i] = new KmeanFeature(len);
        HashMap<Float, Boolean> position = new HashMap<>();

        int size = 0;
        while (size < len * numClus) {
            Float randFloat = (float) Math.random();
            if (!position.containsKey(randFloat)) {
                position.put(randFloat, true);
                centroids[size / len].set(size % len, randFloat);
                size += 1;
            }
        }
        return centroids;
    }

    private KmeanFeature[] readCentroid(String inPath, Configuration conf, int numCluster, KmeanFeature[] old) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] statuses = hdfs.listStatus(new Path(inPath));

        for (int i = 0; i < statuses.length; i++) {
            if (!statuses[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(statuses[i].getPath())));
                while (br.ready()) {
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
