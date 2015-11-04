package com.epam.hadoop.traffic.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/4/2015.
 */
public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] note = line.split(" ");
        if (note[9].equals("-")) {
            context.write(new Text(note[0]), new IntWritable(0));
        } else {
            context.write(new Text(note[0]), new IntWritable(Integer.parseInt(note[9])));
        }
    }
}
