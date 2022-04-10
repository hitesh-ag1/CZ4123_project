package com.ntu.bdm.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NormReducer extends Reducer<Text, Text, Text, Text> {
    int mindate;
    float humMax;
    float humMin;
    float tempMax;
    float tempMin;
    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.mindate = Integer.parseInt(conf.get("date"));
        this.humMax = Float.parseFloat(conf.get("HUMMax"));
        this.humMin = Float.parseFloat(conf.get("HUMMin"));
        this.tempMax = Float.parseFloat(conf.get("TMPMax"));
        this.tempMin = Float.parseFloat(conf.get("TMPMin"));
    }
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String strKey = key.toString();
        String[] listKey = strKey.split(",");
        String station = listKey[0];
        int yearMonth = Integer.parseInt(listKey[1].substring(0, 7));
        String currInd = transformIndex(yearMonth, mindate);
        float floatTemp = 0;
        float floatHum = 0;
        for (Text value : values) {
                String str = value.toString();
                String type = str.substring(str.length() - 1, str.length());
                float floatVal = Float.parseFloat(str.substring(0, str.length() - 1));
                
                if(type.equals("T")){
                    floatTemp = (floatVal-tempMin)/(tempMax-tempMin);
                }
                else if(type.equals("H")){
                    floatHum = (floatVal-humMin)/(humMax-humMin);
                }
//                FloatWritable value2 = new FloatWritable(floatVal);
        }
        String output = String.format("%s,%s",floatTemp,floatHum);
        String newKey = station+","+currInd;
        context.write(new Text(newKey), new Text(output));

//            System.out.printf("Key: %s\n", key);
//            System.out.printf("Max: %s\n",mx);
//            System.out.printf("Min: %s\n",mx);
//            String output = String.format("%s,%s",mx,mn);
//            context.write(key, new Text(output));

    }
    private String transformIndex(int currDate, int minDate){
        int minYear = minDate/100;
        int minMonth = minDate%100;
        int currYear = currDate/100;
        int currMonth = currDate%100;
        int index = (currYear - minYear)*12 + currMonth - minMonth;
        return Integer.toString(index);
    }

}
