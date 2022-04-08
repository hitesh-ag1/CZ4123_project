package com.ntu.bdm.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class KmeanFeature implements Writable {

    // TODO - Initialise to array of size = numMonths
    private float[] array;
    private int numPoints;

    public KmeanFeature(float[] array) {
        this.array = array;
        this.numPoints = 1;
    }

    public KmeanFeature(String string) {
        String[] arr = string.split(",");
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

    public static KmeanFeature duplicate(KmeanFeature p){
        KmeanFeature res = new KmeanFeature(Arrays.copyOf(p.array, p.array.length));
        return res;
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
        int ctr = 0;
        for (int i = 0; i < this.array.length; i++){
            if (!Float.isNaN(point.array[i]) & !Float.isNaN(this.array[i])){
                ctr += 1;
                dist += Math.abs(this.array[i] - point.array[i]);
            }
        }
        return dist / (float) ctr;
    }


    public void calculateCentroid(ArrayList<KmeanFeature> points){
        if (points.isEmpty()) return;
        for (int j = 0; j < points.get(0).array.length; j++){
            float sum = 0F;
            int ctr = 0;
            for (int i = 0; i < points.size(); i++){
                if (!Float.isNaN(points.get(i).array[j])){
                    sum += points.get(i).array[j];
                    ctr += 1;
                }
                this.array[j] = sum / (float) ctr;
            }
        }
        this.numPoints = 1;
    }

    @Override
    public String toString() {
        return  Arrays.toString(array);
    }
}
