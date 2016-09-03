package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class KeywordsCounterMapperTest {
    @Test
    public void mapValid() throws Exception {
        Text value = new Text("282162996094\tcar,digital,cigarette,lossman,fast,charger,usb,double,voltmeter,lighter\tON\tCPC\tBROAD\thttp://www.smth.com/this_one_is_insignificant_for_a_test.html");

        new MapDriver<Object, Text, Text, IntWritable>()
                .withMapper(new KeywordsCounterMapper())
                .withInput(new ShortWritable((short) 0), value)
                .withOutput(new Text("car"), new IntWritable(1))
                .withOutput(new Text("digital"), new IntWritable(1))
                .withOutput(new Text("cigarette"), new IntWritable(1))
                .withOutput(new Text("lossman"), new IntWritable(1))
                .withOutput(new Text("fast"), new IntWritable(1))
                .withOutput(new Text("charger"), new IntWritable(1))
                .withOutput(new Text("usb"), new IntWritable(1))
                .withOutput(new Text("double"), new IntWritable(1))
                .withOutput(new Text("voltmeter"), new IntWritable(1))
                .withOutput(new Text("lighter"), new IntWritable(1))
                .runTest();
    }

}