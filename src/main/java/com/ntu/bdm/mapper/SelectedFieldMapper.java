package com.ntu.bdm.mapper;

import com.ntu.bdm.util.KmeanFeature;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class SelectedFieldMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        String[] keyVal = value.toString().split("\\t");
        String line = keyVal[1];

        String[] oldKey = keyVal[0].split("_");
        String location = oldKey[0];
        String field = oldKey[1];
        String stat = oldKey[2];
        HashMap<String, Integer> selected = new HashMap<>();

//      #TODO This need to be dynamic
        selected.put("HUM_MAX", 0);
        selected.put("TMP_MIN", 1);

        String cur = String.format("%s_%s", field, stat);
        if (selected.containsKey(cur)){
            String newKey = String.format("%s", location);
            String newVal = String.format("%d_%s", selected.get(cur), line);
            context.write(new Text(newKey), new Text(newVal));
        }
    }
}
