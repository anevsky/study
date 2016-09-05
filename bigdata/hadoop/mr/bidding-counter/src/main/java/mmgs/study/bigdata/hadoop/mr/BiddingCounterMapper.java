package mmgs.study.bigdata.hadoop.mr;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class BiddingCounterMapper extends Mapper<Object, Text, Text, HitPriceWritable> {

    private final static Integer one = 1;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        context.write(new Text(tokens[4]), new HitPriceWritable(one, Integer.parseInt(tokens[18])));

        UserAgent userAgent = new UserAgent(tokens[3]);
        String browser = userAgent.getBrowser().getName().replaceAll("[^\\p{Alpha}]", "");
        context.getCounter("Browsers", browser).increment(1);
    }
}
