package com.ntu.bdm.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NormReducer extends Reducer<Text, Text, Text, Text> {
    int mindate;
    float humMax;
    float humMin;
    float tempMax;
    float tempMin;
    float wsMax;
    float wsMin;

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.mindate = conf.getInt("date", 200001);
        this.humMax = conf.getFloat("HUMMax", 0);
        this.humMin = conf.getFloat("HUMMin", 0);
        this.tempMax = conf.getFloat("TMPMax", 0);
        this.tempMin = conf.getFloat("TMPMin", 0);
        this.wsMax = conf.getFloat("WSMax", 0);
        this.wsMin = conf.getFloat("WSMin", 0);

    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String strKey = key.toString();
        String[] listKey = strKey.split(",");
        String station = listKey[0];
        int yearMonth = Integer.parseInt(listKey[1].toString().substring(0, 4) + listKey[1].toString().substring(5, 7));
        String currInd = transformIndex(yearMonth, mindate);
        float floatTemp = 0;
        float floatHum = 0;
        float floatWs = 0;
        for (Text value : values) {
            String str = value.toString();
            String type = str.substring(str.length() - 1, str.length());
            float floatVal = Float.parseFloat(str.substring(0, str.length() - 1));

            switch (type) {
                case "T":
                    floatTemp = (floatVal - tempMin) / (tempMax - tempMin);
                    break;
                case "H":
                    floatHum = (floatVal - humMin) / (humMax - humMin);
                    break;
                case "W":
                    floatWs = (floatVal - wsMin) / (wsMax - wsMin);
                    break;
            }
        }
        String output = String.format("%s,%s,%s", floatTemp, floatHum, floatWs);
        String newValue = station + "," + currInd + ",";
        context.write(new Text("Normalized:"), new Text(newValue + output));
    }

    private String transformIndex(int currDate, int minDate) {
        int minYear = minDate / 100;
        int minMonth = minDate % 100;
        int currYear = currDate / 100;
        int currMonth = currDate % 100;
        int index = (currYear - minYear) * 12 + currMonth - minMonth;
        return Integer.toString(index);
    }

}
