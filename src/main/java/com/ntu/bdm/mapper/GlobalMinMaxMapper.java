package com.ntu.bdm.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class GlobalMinMaxMapper extends Mapper<Object, Text, Text, Text> {
    private Text wordTmp = new Text();
    private Text wordHum = new Text();
    private Text wordWs = new Text();
    private Text wordDate = new Text();

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        System.out.printf("Key: %s \n", key);
        System.out.printf("Value: %s \n", value);

        String line = value.toString().split("\\t")[1];
        String[] stringArr = line.split(",");
        String yearMonth = stringArr[1].substring(0, 7);

        String location = stringArr[0];
        if (stringArr[2].equals("M")) stringArr[2] = "NaN";
        if (stringArr[3].equals("M")) stringArr[3] = "NaN";
        if (stringArr[4].equals("M")) stringArr[4] = "NaN";
        float temp = Float.parseFloat(stringArr[2]);
        float humidity = Float.parseFloat(stringArr[3]);
        float windspeed = Float.parseFloat(stringArr[4]);

        wordTmp.set("TMP");
        context.write(wordTmp, new Text(String.valueOf(temp)));

        wordHum.set("HUM");
        context.write(wordHum, new Text(String.valueOf(humidity)));

        wordWs.set("WS");
        context.write(wordWs, new Text(String.valueOf(windspeed)));

        wordDate.set("DATE");
        context.write(wordDate, new Text(yearMonth));


    }
}
