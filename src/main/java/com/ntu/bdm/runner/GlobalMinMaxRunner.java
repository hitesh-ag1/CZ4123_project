package com.ntu.bdm.runner;

import java.io.IOException;
import java.net.InetAddress;

import com.ntu.bdm.mapper.GlobalMinMaxMapper;
import com.ntu.bdm.reducer.GlobalMinMaxReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GlobalMinMaxRunner {
    public GlobalMinMaxRunner(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String ip = InetAddress.getLocalHost().toString().split("/")[1];
        conf.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "Global Min Max");

        job.setJarByClass(GlobalMinMaxRunner.class);
        job.setMapperClass(GlobalMinMaxMapper.class);
        job.setReducerClass(GlobalMinMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem filesystem = FileSystem.get(conf);
        filesystem.delete(new Path(outPath), true);

        FileInputFormat.addInputPath(job, new Path(inPath));
//      KeyValueTextInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);
    }
}
