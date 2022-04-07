package com.ntu.bdm.runner;

import com.ntu.bdm.mapper.MaxMapper;
import com.ntu.bdm.mapper.OutputMapper;
import com.ntu.bdm.reducer.MaxReducer;
import com.ntu.bdm.reducer.OutputReducer;
import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Arrays;

public class OutputRunner {
    public OutputRunner(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String ip = InetAddress.getLocalHost().toString().split("/")[1];
        conf.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        int numCluster = conf.getInt("numCluster", 3);

        KmeanFeature[] newCentroid = readCentroid("/test/centroid/centroids.txt", conf, numCluster);
        for (int i = 0; i < numCluster; i++) {
            conf.unset("centroid-" + i);
            conf.set("centroid-" + i, newCentroid[i].toString());
        }

        Job job = Job.getInstance(conf, "Final output");

        job.setJarByClass(OutputRunner.class);
        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(OutputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem filesystem = FileSystem.get(conf);
        filesystem.delete(new Path(outPath), true);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private KmeanFeature[] readCentroid(String inPath, Configuration conf, int numCluster) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] statuses = hdfs.listStatus(new Path(inPath));
        KmeanFeature[] centroids = new KmeanFeature[numCluster];

        for (int i = 0; i < statuses.length; i++) {
            if (!statuses[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(statuses[i].getPath())));
                while (br.ready()) {
                    String[] line = br.readLine().split("\\t");
                    int centroidId = Integer.parseInt(line[0]);
                    String centroid = line[1];
                    centroids[centroidId] = new KmeanFeature(centroid.substring(1, centroid.length() - 1));
                }
                br.close();
            }
        }

        return centroids;
    }
}
