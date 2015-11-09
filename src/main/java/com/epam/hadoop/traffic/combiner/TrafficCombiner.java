package com.epam.hadoop.traffic.combiner;

import com.epam.hadoop.models.IntPairWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/5/2015.
 */
public class TrafficCombiner extends Reducer<Text, IntPairWritableComparable, Text, IntPairWritableComparable> {

    public void reduce(Text key, Iterable<IntPairWritableComparable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int amount = 0;
        for (IntPairWritableComparable val : values) {
            sum += val.getLeft();
            amount++;
        }

        context.write(key, new IntPairWritableComparable(sum, amount));
    }
}