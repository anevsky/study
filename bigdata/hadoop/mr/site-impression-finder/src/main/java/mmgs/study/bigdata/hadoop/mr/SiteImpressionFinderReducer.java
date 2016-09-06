package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class SiteImpressionFinderReducer extends Reducer<PinyouidTimestampWritable, NullWritable, PinyouidTimestampWritable, NullWritable> {

    public void reduce(PinyouidTimestampWritable key, Iterable<NullWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
