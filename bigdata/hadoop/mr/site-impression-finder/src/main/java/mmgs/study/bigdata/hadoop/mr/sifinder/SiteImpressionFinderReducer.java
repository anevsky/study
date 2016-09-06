package mmgs.study.bigdata.hadoop.mr.sifinder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

class SiteImpressionFinderReducer extends Reducer<PinyouidTimestampWritable, Text, NullWritable, Text> {

    @Override
    public void reduce(PinyouidTimestampWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
