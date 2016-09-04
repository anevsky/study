package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class BiddingCounterMapper extends Mapper<Object, Text, Text, HitPriceWritable> {

    private final static Integer one = 1;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        context.write(new Text(tokens[4]), new HitPriceWritable(one, Integer.parseInt(tokens[18])));
    }
}
