package com.epam.hadoop.browserstatistic.mapper;

import com.epam.hadoop.compression.model.IntPairWritableComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/6/2015.
 */
public class BrowserStatisticMapper extends Mapper<LongWritable, Text, Text, IntPairWritableComparable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] subStrings = value.toString().split(" ");

    }
}