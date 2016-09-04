package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class BiddingCounterReducerTest {
    @Test
    public void reduce() throws Exception {
        new ReduceDriver<Text, HitPriceWritable, Text, HitPriceWritable>()
                .withReducer(new BiddingCounterReducer())
                .withInput(new Text("202.116.81.*"),
                        Arrays.asList(new HitPriceWritable(1, 300), new HitPriceWritable(1, 50), new HitPriceWritable(1, 20)))
                .withOutput(new Text("202.116.81.*"), new HitPriceWritable(3, 370))
                .runTest();
    }

}