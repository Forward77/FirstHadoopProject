package com.epam.hadoop.compression.executor;

import com.epam.hadoop.compression.combiner.TrafficCombiner;
import com.epam.hadoop.compression.map.TrafficMapper;
import com.epam.hadoop.compression.model.IntPairWritableComparable;
import com.epam.hadoop.compression.reduce.TrafficReducerForCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

        job.setMapperClass(TrafficMapper.class);
        job.setCombinerClass(TrafficCombiner.class);
        job.setReducerClass(TrafficReducerForCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntPairWritableComparable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
