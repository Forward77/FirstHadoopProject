package com.epam.hadoop.traffic.map;

import com.epam.hadoop.traffic.model.IntPairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/4/2015.
 */
public class TrafficMap extends Mapper<LongWritable, Text, Text, IntPairWritable> {
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] note = line.split(" ");
        try {
            context.write(new Text(note[0]), new IntPairWritable(Integer.parseInt(note[9]), 0));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }
}
