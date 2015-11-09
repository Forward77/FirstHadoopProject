package com.epam.hadoop.models;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/4/2015.
 */
public class AmountAndAverage implements Writable {
    private double average;
    private int amount;

    public AmountAndAverage(double average, int amount) {
        this.average = average;
        this.amount = amount;
    }

    public AmountAndAverage() { }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(amount);
        dataOutput.writeDouble(average);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        amount = dataInput.readInt();
        average = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return average + "," + amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AmountAndAverage that = (AmountAndAverage) o;

        if (Double.compare(that.average, average) != 0) return false;
        return amount == that.amount;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(average);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + amount;
        return result;
    }
}
