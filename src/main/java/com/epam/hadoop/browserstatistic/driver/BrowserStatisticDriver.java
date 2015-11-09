package com.epam.hadoop.browserstatistic.driver;

import com.epam.hadoop.browserstatistic.mapper.BrowserStatisticMapper;
import com.epam.hadoop.models.InternetPacket;
import com.epam.hadoop.browserstatistic.reducer.BrowserStatisticReducer;
import com.epam.hadoop.models.AmountAndAverage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by Pavlo_Vitynskyi on 11/6/2015.
 */
public class BrowserStatisticDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");


        Job job = new Job(conf, "traffic");

        job.setJarByClass(BrowserStatisticDriver.class);

        job.setMapperClass(BrowserStatisticMapper.class);
        job.setReducerClass(BrowserStatisticReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(InternetPacket.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(AmountAndAverage.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        /*Counters counters = job.getCounters();
        for (CounterGroup group : counters) {
            System.out.println("- Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
            System.out.println("  number of counters in this group: " + group.size());
            for (Counter counter : group) {
                System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
            }
        }*/
    }
}
