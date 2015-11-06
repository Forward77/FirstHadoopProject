package com.epam.hadoop.traffic.model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/5/2015.
 */
public class IntPairWritableComparable implements WritableComparable<IntPairWritableComparable> {
    private int left;
    private int right;

    public IntPairWritableComparable(int left, int right) {
        this.left = left;
        this.right = right;
    }

    public IntPairWritableComparable() {
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

    @Override
    public int compareTo(IntPairWritableComparable o) {
        if(left == o.left && right == o.right) {
            return 0;
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntPairWritableComparable that = (IntPairWritableComparable) o;

        if (left != that.left) return false;
        return right == that.right;

    }

    @Override
    public int hashCode() {
        int result = left;
        result = 31 * result + right;
        return result;
    }
}
