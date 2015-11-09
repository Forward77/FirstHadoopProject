package com.epam.hadoop.browserstatistic.mapper;

import com.epam.hadoop.browserstatistic.customenum.BrowserEnum;
import com.epam.hadoop.models.InternetPacket;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/6/2015.
 */
public class BrowserStatisticMapper extends Mapper<LongWritable, Text, IntWritable, InternetPacket> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] subStrings = value.toString().split(" ");
        try {
            int id = Integer.parseInt(subStrings[0].substring(2));
            int traffic = Integer.parseInt(subStrings[9]);
            String browser = subStrings[11].split("\"|/")[1];
            InternetPacket internetPacket = new InternetPacket(id, browser, traffic);
            context.write(new IntWritable(id), internetPacket);
        } catch (Exception e) {
            e.printStackTrace();
            context.getCounter(BrowserEnum.BAD_DATA.toString(), "BAD_LINE").increment(1);
        }
    }
}