package com.ntu.bdm.runner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;

import com.ntu.bdm.mapper.NormMapper;
import com.ntu.bdm.reducer.NormReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NormRunner {
    public NormRunner(String inPath, String outPath, String inPath2) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String ip = InetAddress.getLocalHost().toString().split("/")[1];
        conf.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        getMinMax(inPath2, conf);
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


//        int mindate = conf.getInt("date", 200001);
//        float humMax = conf.getFloat("HUMMax", 0);
//        float humMin = conf.getFloat("HUMMin", 0);
//        float tempMax = conf.getFloat("TMPMax", 0);
//        float tempMin = conf.getFloat("TMPMin", 0);
//        System.out.println("TestMinDate "+ mindate);
//        System.out.println("HUMMax "+ humMax);
        job.waitForCompletion(true);
    }

    private void getMinMax(String inPath2, Configuration conf) throws IOException {
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
    }
}
