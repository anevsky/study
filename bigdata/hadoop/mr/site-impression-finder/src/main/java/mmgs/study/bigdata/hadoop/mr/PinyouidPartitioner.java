package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PinyouidPartitioner extends Partitioner<PinyouidTimestampWritable, NullWritable> {
    @Override
    public int getPartition(PinyouidTimestampWritable pinyouidTimestampWritable, NullWritable nullWritable, int i) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
