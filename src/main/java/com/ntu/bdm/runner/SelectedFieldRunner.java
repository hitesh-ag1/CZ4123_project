package com.ntu.bdm.runner;

import com.ntu.bdm.mapper.PointMapper;
import com.ntu.bdm.mapper.SelectedFieldMapper;
import com.ntu.bdm.reducer.PointReducer;
import com.ntu.bdm.reducer.SelectedFieldReducer;
import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


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
    public SelectedFieldRunner(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://3.1.36.136:9000");
        conf.set("yarn.resourcemanager.hostname", "3.1.36.136"); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "FlattenFields");

        job.setJarByClass(SelectedFieldRunner.class);
        job.setMapperClass(SelectedFieldMapper.class);
        job.setReducerClass(SelectedFieldReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(KmeanFeature.class);

        FileSystem filesystem = FileSystem.get(conf);
        filesystem.delete(new Path(outPath), true);

        FileInputFormat.addInputPath(job, new Path(inPath));
//      KeyValueTextInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
