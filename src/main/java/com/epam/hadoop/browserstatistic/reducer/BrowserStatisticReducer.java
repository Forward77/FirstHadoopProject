package com.epam.hadoop.browserstatistic.reducer;

import com.epam.hadoop.browserstatistic.customenum.BrowserEnum;
import com.epam.hadoop.models.AmountAndAverage;
import com.epam.hadoop.models.InternetPacket;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Pavlo_Vitynskyi on 11/6/2015.
 */
public class BrowserStatisticReducer extends Reducer<IntWritable, InternetPacket, IntWritable, AmountAndAverage> {

    public void reduce(IntWritable key, Iterable<InternetPacket> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int amount = 0;
        Set<String> browsers = new HashSet<>();
        for (InternetPacket packet : values) {
            sum += packet.getData();
            amount++;
            if (!browsers.contains(packet.getBrowser())) {
                context.getCounter(BrowserEnum.BROWSER.toString(), packet.getBrowser()).increment(1);
                browsers.add(packet.getBrowser());
            }
        }
        double average = (double)sum / amount;
        context.write(key, new AmountAndAverage(average, sum));
    }
}