package com.epam.hadoop.traffic.combiner;

import com.epam.hadoop.traffic.model.AmountAndAverage;
import com.epam.hadoop.traffic.model.IntPairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/5/2015.
 */
public class TrafficCombiner extends Reducer<Text, IntPairWritable, Text, IntPairWritable> {

    public void reduce(Text key, Iterable<IntPairWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int amount = 0;
        for (IntPairWritable val : values) {
            sum += val.getLeft();
            amount++;
        }

        context.write(key, new IntPairWritable(sum, amount));
    }
}