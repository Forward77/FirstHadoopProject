package com.epam.hadoop.traffic.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/4/2015.
 */
public class Reduce extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int amount = 0;
        for (IntWritable val : values) {
            sum += val.get();
            amount++;
        }
        double average = (double)sum / amount;
        context.write(key, new Text(average + ", " + sum));
    }
}
