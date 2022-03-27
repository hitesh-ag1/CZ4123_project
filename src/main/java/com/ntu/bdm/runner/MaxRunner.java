package com.ntu.bdm.runner;

import java.io.IOException;

import com.ntu.bdm.mapper.MaxMapper;
import com.ntu.bdm.reducer.MaxReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://3.1.36.136:9000");
        conf.set("yarn.resourcemanager.hostname", "3.1.36.136"); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        String inPath = args[0];
        String outPath = args[1];
        Job job = Job.getInstance(conf, "Max Temp");
        job.setJarByClass(MaxRunner.class);
        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
