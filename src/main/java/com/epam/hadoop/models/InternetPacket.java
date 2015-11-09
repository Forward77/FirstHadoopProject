package com.epam.hadoop.models;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/9/2015.
 */
public class InternetPacket implements Writable {
    private int userId;
    private String browser;
    private int data;

    public InternetPacket(){}

    public InternetPacket(int userId, String browser, int data) {
        this.userId = userId;
        this.browser = browser;
        this.data = data;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public int getData() {
        return data;
    }

    public void setData(int data) {
        this.data = data;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(userId);
        dataOutput.writeInt(data);
        dataOutput.writeChars(browser);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId = dataInput.readInt();
        data = dataInput.readInt();
        browser = dataInput.readLine();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InternetPacket that = (InternetPacket) o;

        if (userId != that.userId) return false;
        if (data != that.data) return false;
        return browser.equals(that.browser);

    }

    @Override
    public int hashCode() {
        int result = userId;
        result = 31 * result + browser.hashCode();
        result = 31 * result + data;
        return result;
    }

    @Override
    public String toString() {
        return "InternetPacket{" +
                "userId=" + userId +
                ", browser='" + browser + '\'' +
                ", data=" + data +
                '}';
    }
}
