package com.ntu.bdm.runner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.ntu.bdm.mapper.NormMapper;
import com.ntu.bdm.reducer.NormReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NormRunner {
    public NormRunner(String inPath, String outPath, String inPath2) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://54.169.249.35:9000");
        conf.set("yarn.resourcemanager.hostname", "54.169.249.35"); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "Normalization");

        job.setJarByClass(NormRunner.class);
        job.setMapperClass(NormMapper.class);
        job.setReducerClass(NormReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem filesystem = FileSystem.get(conf);
        filesystem.delete(new Path(outPath), true);

        FileInputFormat.addInputPath(job, new Path(inPath));
//      KeyValueTextInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        conf = getMinMax(inPath2, conf);
        int mindate = Integer.parseInt(conf.get("date"));
        float humMax = Float.parseFloat(conf.get("HUMMax"));
        float humMin = Float.parseFloat(conf.get("HUMMin"));
        float tempMax = Float.parseFloat(conf.get("TMPMax"));
        float tempMin = Float.parseFloat(conf.get("TMPMin"));
        System.out.println("TestMinDate"+ mindate);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private Configuration getMinMax(String inPath2, Configuration conf) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] statuses = hdfs.listStatus(new Path(inPath2));
//        System.out.println("Reading MinMax");

        for (int i = 0; i < statuses.length; i++) {
            if (!statuses[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(statuses[i].getPath())));
                while (br.ready()) {
                    String[] line = br.readLine().split("\\t");
//                    System.out.println(line[0]);
                    switch (line[0]) {
                        case "DATE":
                            String[] range2 = line[1].split(",");
                            System.out.println("date " + range2[1]);
                            conf.set("date", range2[1]);
                            break;

                        default:
                            String[] range = line[1].split(",");
                            System.out.println(line[0] + "Max " + range[0]);
                            System.out.println(line[0] + "Min " + range[1]);

                            conf.set(line[0] + "Max", range[0]);
                            conf.set(line[0] + "Min", range[1]);
                            break;
                    }

                }
                    br.close();
                }
            }
        return conf;

    }
}
