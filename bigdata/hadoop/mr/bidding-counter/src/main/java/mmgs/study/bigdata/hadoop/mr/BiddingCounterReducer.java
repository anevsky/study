package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class BiddingCounterReducer extends Reducer<Text, HitPriceWritable, Text, HitPriceWritable> {
    private HitPriceWritable result = new HitPriceWritable();

    public void reduce(Text key, Iterable<HitPriceWritable> values, Context context) throws IOException, InterruptedException {
        int sumHit = 0;
        int sumPrice = 0;
        for (HitPriceWritable val : values) {
            sumHit += val.getHit().get();
            sumPrice += val.getPrice().get();
        }
        result.set(sumHit, sumPrice);
        context.write(key, result);
    }
}
