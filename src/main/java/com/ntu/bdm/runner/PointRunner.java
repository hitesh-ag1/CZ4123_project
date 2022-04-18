package com.ntu.bdm.runner;

import com.ntu.bdm.mapper.PointMapper;
import com.ntu.bdm.reducer.PointReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;


/*
 * Input k,v = 1_SG_TMP: 0.4, 0.7, 0.6
 * If the v length is 2 then minmax else mean, median, sd
 *
 * Map to k,v = SG_TMP_MEAN: 1_0.5
 * # bef _ is the yearMonth index
 *
 * Output k,v = SG_TMP_MEAN: 0.1, 0.3, 0.7.....
 * Length of value = #year * 12
 * Use the index in map stage to identify the location in array
 */
public class PointRunner {
    public PointRunner(String inPath, String outPath, String globalStats) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String ip = InetAddress.getLocalHost().toString().split("/")[1];
        conf.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        int numMonth = getNumMonth(globalStats, conf);
        conf.setInt("numMonth", numMonth);

        Job job = Job.getInstance(conf, "ConvertToPoint");

        job.setJarByClass(PointRunner.class);
        job.setMapperClass(PointMapper.class);
        job.setReducerClass(PointReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem filesystem = FileSystem.get(conf);
        filesystem.delete(new Path(outPath), true);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);
    }

    private int getNumMonth(String inPath, Configuration conf) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] statuses = hdfs.listStatus(new Path(inPath));

        for (int i = 0; i < statuses.length; i++) {
            if (!statuses[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(statuses[i].getPath())));
                while (br.ready()) {
                    String[] line = br.readLine().split("\\t");
                    if (line[0].equals("DATE")) {
                        br.close();
                        String[] range2 = line[1].split(",");
                        // (maxYr - minYr) * 12 + maxMonth - minMonth + 1
                        return ((Integer.parseInt(range2[0].substring(0, 4)) -
                                Integer.parseInt(range2[1].substring(0, 4))) * 12) +
                                Integer.parseInt(range2[0].substring(4, 6)) -
                                Integer.parseInt(range2[1].substring(4, 6))  + 1;
                    }
                }
                br.close();
            }
        }
        return -1;

    }
}
