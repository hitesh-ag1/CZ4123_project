package com.ntu.bdm.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class KmeanFeature implements Writable {

    private float[] array = null;
    private int numPoints;

    public KmeanFeature(float[] array) {
        this.array = array;
        this.numPoints = 1;
    }

    public KmeanFeature(String[] arr) {
        this.array = new float[arr.length];
        for (int i = 0; i < arr.length; i++){
            this.array[i] = Float.parseFloat(arr[i]);
        }
        this.numPoints = 1;
    }

    public KmeanFeature(int len){
        this.array = new float[len];
    }

    public void set(int idx, float val){
        this.array[idx] = val;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (float f: array){
            dataOutput.writeFloat(f);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < this.array.length; i++){
            array[i] = dataInput.readFloat();
        }
    }

    public float distance(KmeanFeature point){
        float dist = 0F;
        for (int i = 0; i < this.array.length; i++){
            dist += (this.array[i] - point.array[i]);
        }
        return dist;
    }


    private void calculateCentroid(KmeanFeature[] points){
        for (int j = 0; j < points[0].array.length; j++){
            float sum = 0F;
            int ctr = 0;
            for (int i = 0; i < points.length; i++){
                sum += points[i].array[j];
                if (!Float.isNaN(points[i].array[j])){
                    ctr += 1;
                }
            this.array[j] = sum / ctr;
        }
        }
        this.numPoints = 1;
    }

    @Override
    public String toString() {
        return Arrays.toString(array);
    }
}
