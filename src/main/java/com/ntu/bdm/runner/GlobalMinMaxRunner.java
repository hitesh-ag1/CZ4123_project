package com.ntu.bdm.runner;

import java.io.IOException;

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
        conf.set("fs.default.name","hdfs://54.169.249.35:9000");
        conf.set("yarn.resourcemanager.hostname", "54.169.249.35"); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "Min Max Temp");

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
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
