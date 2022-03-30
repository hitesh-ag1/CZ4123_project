package com.ntu.bdm.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KmeanFeature implements Writable {

    private float[] array = null;

    public KmeanFeature(float[] array) {
        this.array = array;
    }

    public KmeanFeature(String string) {
        String[] arr = string.split(",");
        this.array = new float[arr.length];
        for (int i = 0; i < arr.length; i++){
            this.array[i] = Float.parseFloat(arr[i]);
        }
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

    private float distance(KmeanFeature point){
        float dist = 0F;
        for (int i = 0; i < this.array.length; i++){
            dist += (this.array[i] - point.array[i]);
        }
        return dist;
    }
}
