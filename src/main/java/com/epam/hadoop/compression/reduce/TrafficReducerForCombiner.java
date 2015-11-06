package com.epam.hadoop.compression.reduce;

import com.epam.hadoop.compression.model.AmountAndAverage;
import com.epam.hadoop.compression.model.IntPairWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/5/2015.
 */
public class TrafficReducerForCombiner extends Reducer<Text, IntPairWritableComparable, Text, AmountAndAverage> {

    public void reduce(Text key, Iterable<IntPairWritableComparable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int amount = 0;
        for (IntPairWritableComparable val : values) {
            sum += val.getLeft();
            amount += val.getRight();
        }
        double average = (double)sum / amount;
        context.write(key, new AmountAndAverage(average, sum));
    }
}
