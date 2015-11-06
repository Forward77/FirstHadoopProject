package com.epam.hadoop.compression.executor;

import com.epam.hadoop.compression.combiner.TrafficCombiner;
import com.epam.hadoop.compression.map.TrafficMapper;
import com.epam.hadoop.compression.model.AmountAndAverage;
import com.epam.hadoop.compression.model.IntPairWritableComparable;
import com.epam.hadoop.compression.reduce.TrafficReducerForCombiner;
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

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntPairWritableComparable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AmountAndAverage.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
