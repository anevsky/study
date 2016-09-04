package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import static org.junit.Assert.*;

public class BiddingCounterMapperTest {
    @Test
    public void map() throws Exception {
        Text value = new Text("b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104000\tVh16OwT6OQNUXbj\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282825712746\t0");

        new MapDriver<Object, Text, Text, HitPriceWritable>()
                .withMapper(new BiddingCounterMapper())
                .withInput(new ShortWritable((short) 0), value)
                .withOutput(new Text("180.127.189.*"), new HitPriceWritable(1, 227))
                .runTest();
    }

}