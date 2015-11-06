package com.epam.hadoop.browserstatistic.reducer;

import com.epam.hadoop.compression.model.AmountAndAverage;
import com.epam.hadoop.compression.model.IntPairWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Pavlo_Vitynskyi on 11/6/2015.
 */
public class BrowserStatisticReducer extends Reducer<Text, IntPairWritableComparable, Text, AmountAndAverage> {

    public void reduce(Text key, Iterable<IntPairWritableComparable> values, Context context)
            throws IOException, InterruptedException {

    }
}