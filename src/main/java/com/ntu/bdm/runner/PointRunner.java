package com.ntu.bdm.runner;

import com.ntu.bdm.mapper.MaxMapper;
import com.ntu.bdm.mapper.PointMapper;
import com.ntu.bdm.reducer.MaxReducer;
import com.ntu.bdm.reducer.PointReducer;
import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


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
    public PointRunner(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://3.1.36.136:9000");
        conf.set("yarn.resourcemanager.hostname", "3.1.36.136"); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "ConvertToPoint");

        job.setJarByClass(PointRunner.class);
        job.setMapperClass(PointMapper.class);
        job.setReducerClass(PointReducer.class);

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
