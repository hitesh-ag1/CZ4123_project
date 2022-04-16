package com.ntu.bdm.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class NormMapper extends Mapper<Object, Text, Text, Text> {
    int mindate;
    private Text wordTmp = new Text();
    private Text wordHum = new Text();
    private Text wordWs = new Text();


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.mindate = conf.getInt("date", 200001);
        System.out.println("TestMinDate Mapper " + mindate);
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        System.out.printf("Key: %s \n", key);
        System.out.printf("Value: %s \n", value);

        String line = value.toString().split("\\t")[1];
        String[] stringArr = line.split(",");
        String station = stringArr[0];
        String yearMonth = stringArr[1];
        if (stringArr[2].equals("M")) stringArr[2] = "NaN";
        if (stringArr[3].equals("M")) stringArr[3] = "NaN";
        if (stringArr[4].equals("M")) stringArr[4] = "NaN";

        float temp = Float.parseFloat(stringArr[2]);
        float humidity = Float.parseFloat(stringArr[3]);
        float windspeed = Float.parseFloat(stringArr[4]);


        wordTmp.set((station + "," + yearMonth));
        context.write(wordTmp, new Text(String.valueOf(temp) + "T"));

        wordHum.set((station + "," + yearMonth));
        context.write(wordHum, new Text(String.valueOf(humidity) + "H"));

        wordWs.set((station + "," + yearMonth));
        context.write(wordWs, new Text(String.valueOf(windspeed) + "W"));

    }
}
