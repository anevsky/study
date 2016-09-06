package mmgs.study.bigdata.hadoop.mr.sifinder;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static mmgs.study.bigdata.hadoop.mr.sifinder.SiteImpressionFinderConstants.*;

class SiteImpressionFinderReducer extends Reducer<PinyouidTimestampWritable, Text, NullWritable, Text> {

    private long max;
    private String maxPinYouId;

    private long curr;
    private String currPinYouId;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.max = -1;
        this.curr = 0;
        this.currPinYouId = "";
        this.maxPinYouId = "";
    }

    @Override
    public void reduce(PinyouidTimestampWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(NullWritable.get(), value);
            String line = value.toString();
            String streamId = line.substring(line.lastIndexOf(MAPPER_DELIMITER) + 1);
            String pinYouId = key.getPinyouid().toString();
            if (APP_SITE_IMPRESSION_CODE.equals(streamId)) {
                if (this.currPinYouId.equals(pinYouId)) {
                    this.curr++;
                } else {
                    if (this.curr > this.max) {
                        this.max = this.curr;
                        this.maxPinYouId = this.currPinYouId;
                    }
                    this.curr = 1;
                    this.currPinYouId = pinYouId;
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) {
        if (this.curr > this.max) {
            this.max = this.curr;
            this.maxPinYouId = this.currPinYouId;
        }
        if (!"".equals(this.maxPinYouId))
            context.getCounter(APP_SITE_IMPRESSION_GROUP, this.maxPinYouId).setValue(this.max);
    }
}
