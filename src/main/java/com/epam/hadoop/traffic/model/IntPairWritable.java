package com.epam.hadoop.traffic.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/5/2015.
 */
public class IntPairWritable implements Writable {
    private int left;
    private int right;

    public IntPairWritable(int left, int right) {
        this.left = left;
        this.right = right;
    }

    public IntPairWritable() {
    }

    public int getLeft() {
        return left;
    }

    public void setLeft(int left) {
        this.left = left;
    }

    public int getRight() {
        return right;
    }

    public void setRight(int right) {
        this.right = right;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(left);
        dataOutput.writeInt(right);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        left = dataInput.readInt();
        right = dataInput.readInt();
    }
}
