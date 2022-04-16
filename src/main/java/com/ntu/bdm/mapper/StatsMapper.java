package com.ntu.bdm.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class StatsMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text wordTmp = new Text();
    private Text wordHum = new Text();
    private Text wordWs = new Text();


    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        System.out.printf("Key: %s \n", key);
        System.out.printf("Value: %s \n", value);

        String line = value.toString().split("\\t")[1];
        String[] stringArr = line.split(",");
        String yearMonth = stringArr[1];
        String location = stringArr[0];
        if (stringArr[2].equals("M")) stringArr[2] = "NaN";
        if (stringArr[3].equals("M")) stringArr[3] = "NaN";
        if (stringArr[4].equals("M")) stringArr[4] = "NaN";
        float temp = Float.parseFloat(stringArr[2]);
        float humidity = Float.parseFloat(stringArr[3]);
        float windspeed = Float.parseFloat(stringArr[4]);



        wordTmp.set(yearMonth + "_" + location + "_TMP");
        context.write(wordTmp, new FloatWritable(temp));

        wordHum.set(yearMonth + "_" + location + "_HUM");
        context.write(wordHum, new FloatWritable(humidity));

        wordHum.set(yearMonth + "_" + location + "_WS");
        context.write(wordWs, new FloatWritable(windspeed));
    }
}
