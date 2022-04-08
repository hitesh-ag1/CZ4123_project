package com.ntu.bdm.runner;

import com.ntu.bdm.mapper.SelectedFieldMapper;
import com.ntu.bdm.reducer.SelectedFieldReducer;
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
import java.util.ArrayList;


/*
 * Input k,v = SG_TMP_MEAN: 0.1, 0.3, 0.7.....
 * The mapper will filter the k and only keep the selected one
 *
 * Map to k,v = SG: 1_0.1,0.2...
 *              SG: 2_0.5,0.6....
 * The number before _ is the position of selected field
 * to ensure all the reduced value follow the same sequence
 *
 * Output k,v = SG: 0.1, 0.3, 0.7....., 0.4, 0.5, 0.6....
 * Length of value = #year * 12 * #Field
 * All the v of the same location will be flattened into 1D array
 * following the index in map stage
 */

public class SelectedFieldRunner {
    public SelectedFieldRunner(String inPath, String outPath, ArrayList<String> criterion) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String ip = InetAddress.getLocalHost().toString().split("/")[1];
        conf.set("fs.default.name", String.format("hdfs://%s:9000", ip));
        conf.set("yarn.resourcemanager.hostname", ip); // see step 3
        conf.set("mapreduce.framework.name", "yarn");

        String s = String.valueOf(criterion);
        conf.set("criterion", s.substring(1, s.length() - 1));
        conf.setInt("numCriterion", criterion.size());

        int numMonthPerCriteria = getMonth(inPath, conf);
        conf.setInt("numMonthPerCriteria", numMonthPerCriteria);

        Job job = Job.getInstance(conf, "FlattenFields");

        job.setJarByClass(SelectedFieldRunner.class);
        job.setMapperClass(SelectedFieldMapper.class);
        job.setReducerClass(SelectedFieldReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem filesystem = FileSystem.get(conf);
        filesystem.delete(new Path(outPath), true);

        FileInputFormat.addInputPath(job, new Path(inPath));
//      KeyValueTextInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);
    }

    private int getMonth(String inPath, Configuration conf) throws IOException {
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
