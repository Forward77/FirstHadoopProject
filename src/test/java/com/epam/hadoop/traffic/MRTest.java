package com.epam.hadoop.traffic;

import com.epam.hadoop.traffic.mapper.TrafficMapper;
import com.epam.hadoop.traffic.model.AmountAndAverage;
import com.epam.hadoop.traffic.model.IntPairWritableComparable;
import com.epam.hadoop.traffic.reducer.TrafficReducerForCombiner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Pavlo_Vitynskyi on 11/6/2015.
 */
public class MRTest {
    MapDriver<LongWritable, Text, Text, IntPairWritableComparable> mapDriver;
    ReduceDriver<Text, IntPairWritableComparable, Text, AmountAndAverage> reduceDriver;
    //MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        TrafficMapper mapper = new TrafficMapper();
        TrafficReducerForCombiner reducer = new TrafficReducerForCombiner();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        //mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip537 - - [25/Apr/2011:15:06:27 -0400] \"GET /favicon.ico HTTP/1.1\" 200 318 \"-\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB6.6; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; OfficeLiveConnector.1.5; OfficeLivePatch.1.3)\""));
        mapDriver.withOutput(new Text("ip537"), new IntPairWritableComparable(318, 0));
        mapDriver.runTest();
    }

   @Test
    public void testReducer() {
        List<IntPairWritableComparable> values = new ArrayList<>();
        values.add(new IntPairWritableComparable(1000, 2));
        values.add(new IntPairWritableComparable(500, 1));
        reduceDriver.withInput(new Text("id100"), values);
        reduceDriver.withOutput(new Text("id100"), new AmountAndAverage(500, 1500));
        reduceDriver.runTest();
    }
}
