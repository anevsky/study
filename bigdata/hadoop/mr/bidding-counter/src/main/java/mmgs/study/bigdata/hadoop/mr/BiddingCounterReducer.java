package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BiddingCounterReducer extends Reducer<Text, HitPriceWritable, Text, HitPriceWritable> {
    private HitPriceWritable result = new HitPriceWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    }
}
