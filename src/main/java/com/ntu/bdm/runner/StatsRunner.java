package com.ntu.bdm.runner;

import com.ntu.bdm.mapper.DummyMapper;
import com.ntu.bdm.mapper.StatsMapper;
import com.ntu.bdm.reducer.MeanMedSDReducer;
import com.ntu.bdm.reducer.MinMaxReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.InetAddress;

public class StatsRunner {
    public StatsRunner(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String ip = InetAddress.getLocalHost().toString().split("/")[1];
        conf.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "Map for stats");

        job.setJarByClass(StatsRunner.class);
        job.setMapperClass(StatsMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileSystem filesystem = FileSystem.get(conf);
        String tmpOut = "/tmp/stats/map";
        filesystem.delete(new Path(tmpOut), true);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(tmpOut));
        job.waitForCompletion(true);


//        Second task - Min Max
        Configuration conf2 = new Configuration();
        conf2.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf2.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf2.set("mapreduce.framework.name", "yarn");
        Job job2 = Job.getInstance(conf2, "Min Max");

        job2.setJarByClass(StatsRunner.class);
        job2.setMapperClass(DummyMapper.class);
        job2.setReducerClass(MinMaxReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

        String minMaxOut = outPath + "/minMax";
        filesystem.delete(new Path(minMaxOut), true);

        FileInputFormat.addInputPath(job2, new Path("/tmp/stats/map"));
        FileOutputFormat.setOutputPath(job2, new Path(minMaxOut));
        job2.waitForCompletion(true);


        //        Third task - Mean Median SD
        Configuration conf3 = new Configuration();
        conf3.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf3.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf3.set("mapreduce.framework.name", "yarn");
        Job job3 = Job.getInstance(conf3, "Mean Median SD");

        job3.setJarByClass(StatsRunner.class);
        job3.setMapperClass(DummyMapper.class);
        job3.setReducerClass(MeanMedSDReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(FloatWritable.class);

        String mmSDOut = outPath + "/meanMedSD";
        filesystem.delete(new Path(mmSDOut), true);

        FileInputFormat.addInputPath(job3, new Path("/tmp/stats/map"));
        FileOutputFormat.setOutputPath(job3, new Path(mmSDOut));
        job3.waitForCompletion(true);
    }
}
