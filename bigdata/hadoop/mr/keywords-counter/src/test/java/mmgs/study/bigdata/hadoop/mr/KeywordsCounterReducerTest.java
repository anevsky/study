package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class KeywordsCounterReducerTest {
    @Test
    public void reduceValid() throws IOException, InterruptedException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new KeywordsCounterReducer())
                .withInput(new Text("car"),
                        Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(1)))
                .withOutput(new Text("car"), new IntWritable(3))
                .runTest();
    }

}