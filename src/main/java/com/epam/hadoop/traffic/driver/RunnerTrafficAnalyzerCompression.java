package com.epam.hadoop.traffic.driver;

import com.epam.hadoop.traffic.combiner.TrafficCombiner;
import com.epam.hadoop.traffic.mapper.TrafficMapper;
import com.epam.hadoop.models.IntPairWritableComparable;
import com.epam.hadoop.traffic.reducer.TrafficReducerForCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Created by Pavlo_Vitynskyi on 11/6/2015.
 */
public class RunnerTrafficAnalyzerCompression {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");


        Job job = new Job(conf, "traffic");

        job.setJarByClass(RunnerTrafficAnalyzer.class);

        job.setMapperClass(TrafficMapper.class);
        job.setCombinerClass(TrafficCombiner.class);
        job.setReducerClass(TrafficReducerForCombiner.class);

        //SequenceFileOutputFormat.setOutputPath(job, new Path(args[2]));
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntPairWritableComparable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
