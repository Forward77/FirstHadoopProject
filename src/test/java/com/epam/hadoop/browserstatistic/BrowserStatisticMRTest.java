package com.epam.hadoop.browserstatistic;

import com.epam.hadoop.browserstatistic.mapper.BrowserStatisticMapper;
import com.epam.hadoop.browserstatistic.reducer.BrowserStatisticReducer;
import com.epam.hadoop.models.AmountAndAverage;
import com.epam.hadoop.models.IntPairWritableComparable;
import com.epam.hadoop.models.InternetPacket;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Pavlo_Vitynskyi on 11/9/2015.
 */
public class BrowserStatisticMRTest {
    MapDriver<LongWritable, Text, IntWritable, InternetPacket> mapDriver;
    ReduceDriver<IntWritable, InternetPacket, IntWritable, AmountAndAverage> reduceDriver;
    //MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        BrowserStatisticMapper mapper = new BrowserStatisticMapper();
        BrowserStatisticReducer reducer = new BrowserStatisticReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        //mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip537 - - [25/Apr/2011:15:06:27 -0400] \"GET /favicon.ico HTTP/1.1\" 200 318 \"-\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB6.6; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; OfficeLiveConnector.1.5; OfficeLivePatch.1.3)\""));
        mapDriver.withOutput(new IntWritable(537), new InternetPacket(537, "Mozilla", 318));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        /*List<IntPairWritableComparable> values = new ArrayList<>();
        values.add(new InternetPacket(1000, 2));
        values.add(new InternetPacket(500, 1));
        reduceDriver.withInput(new IntWritable(100), values);
        reduceDriver.withOutput(new IntWritable(100), new AmountAndAverage(500, 1500));
        reduceDriver.runTest();*/
    }
}
