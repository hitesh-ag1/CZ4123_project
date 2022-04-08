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

    public KmeanRunner(String inPath, String outPath, int numCluster, int numIteration, boolean randomCentroidInit) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String ip = InetAddress.getLocalHost().toString().split("/")[1];
        conf.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf.set("mapreduce.framework.name", "yarn");

        boolean stop = false;

        int lengthOfFeature = this.getLengthOfFeature(inPath, conf);
        conf.setInt("numCluster", numCluster);
        conf.setInt("lengthOfFeature", lengthOfFeature);
        KmeanFeature[] newCentroid;
        if (randomCentroidInit)
            newCentroid = generateCentroid(lengthOfFeature, numCluster);
        else newCentroid = readPointAsCentroid(inPath, conf, numCluster);

        for (int i = 0; i < numCluster; i++) {
            conf.unset("centroid-" + i);
            conf.set("centroid-" + i, newCentroid[i].toString());
        }


        int ctr = 0;
        while (!stop) {
            outPath += "_" + ctr;
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
        writeCentroid(conf, newCentroid, "/tmp");

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

    private KmeanFeature[] readPointAsCentroid(String inPath, Configuration conf, int numCluster) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] statuses = hdfs.listStatus(new Path(inPath));
        KmeanFeature[] centroids = new KmeanFeature[numCluster];
        int len = conf.getInt("lengthOfFeature", 12);
        for (int i = 0; i < numCluster; i++){
            centroids[i] = new KmeanFeature(len);
        }


        for (int i = 0; i < statuses.length; i++) {
            if (!statuses[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(statuses[i].getPath())));
                int centroidId = 0;
                while (br.ready()) {
                    String[] line = br.readLine().split("\\t");
                    String centroid = line[1];
                    centroids[centroidId] = new KmeanFeature(centroid.substring(1, centroid.length() - 1));
                    centroidId += 1;
                    if (centroidId >= numCluster){break;}
                }
                br.close();
            }
        }
        return centroids;
    }

    private int getLengthOfFeature(String inPath, Configuration conf) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] statuses = hdfs.listStatus(new Path(inPath));
        int len = 0;
        for (int i = 0; i < statuses.length; i++) {
            if (!statuses[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(statuses[i].getPath())));
                String[] line = br.readLine().split("\\t");
                String point = line[1];
                len = point.substring(1, point.length() - 1).split(",").length;
                br.close();
            }
        }
        return len;
    }
}
