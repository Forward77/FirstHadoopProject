package com.epam.hadoop.traffic.executor;

import com.epam.hadoop.traffic.combiner.TrafficCombiner;
import com.epam.hadoop.traffic.map.TrafficMap;
import com.epam.hadoop.traffic.model.IntPairWritable;
import com.epam.hadoop.traffic.reduce.TrafficReduce;
import com.epam.hadoop.traffic.reduce.TrafficReducerForCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by Pavlo_Vitynskyi on 11/4/2015.
 */
public class RunnerTrafficAnalyzer {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job job = new Job(conf, "traffic");

        job.setJarByClass(RunnerTrafficAnalyzer.class);

        job.setMapperClass(TrafficMap.class);
        job.setCombinerClass(TrafficCombiner.class);
        job.setReducerClass(TrafficReducerForCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntPairWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
