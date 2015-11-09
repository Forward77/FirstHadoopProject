package com.epam.hadoop.compression.reducer;

import com.epam.hadoop.models.AmountAndAverage;
import com.epam.hadoop.models.IntPairWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/4/2015.
 */
public class TrafficReducer extends Reducer<Text, IntPairWritableComparable, Text, AmountAndAverage> {

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
