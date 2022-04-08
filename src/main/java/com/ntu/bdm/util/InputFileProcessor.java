package com.ntu.bdm.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InputFileProcessor extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <output> <record length>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

//  Set RecordLenght configuration parameter so that is it accessible to
//  individual mappers
        Configuration conf = getConf();
        conf.set("yarn.resourcemanager.hostname", "54.169.249.35"); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("fs.default.name","hdfs://54.169.249.35:9000");
        String inPath = args[0];
        String outPath = args[1];
        Job job = Job.getInstance(conf, "Input Format");

//  We do not need reducers for this demonstration
        job.setNumReduceTasks(0);

//  We need all splits, except last, to be multiple of record length.  This is
//  only way to ensure we are not troubled by split boundaries.
//  Set InputFormat to our customised input format.
        job.setInputFormatClass(LineNumInputFormat.class);
        job.setJarByClass(getClass());

//  Set your input and output path and delete output directory.
        TextInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        FileSystem filesystem = FileSystem.get(getConf());
        filesystem.delete(new Path(outPath), true);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new InputFileProcessor(), args);
        System.exit(exitCode);
    }
}
