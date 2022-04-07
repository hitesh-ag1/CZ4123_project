package com.ntu.bdm.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;


public class SelectedFieldMapper extends Mapper<Object, Text, Text, Text> {
    private String[] criterion;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String s = context.getConfiguration().get("criterion");
        criterion = s.split(",");
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        String[] keyVal = value.toString().split("\\t");
        String line = keyVal[1];

        String[] oldKey = keyVal[0].split("_");
        String location = oldKey[0];
        String field = oldKey[1];
        String stat = oldKey[2];
        HashMap<String, Integer> selected = new HashMap<>();

        for (int i = 0; i < criterion.length; i++) {
            selected.put(criterion[i], i);
        }

        String cur = String.format("%s_%s", field, stat);
        if (selected.containsKey(cur)) {
            String newKey = String.format("%s", location);
            String newVal = String.format("%d_%s", selected.get(cur), line);
            context.write(new Text(newKey), new Text(newVal));
        }
    }
}
